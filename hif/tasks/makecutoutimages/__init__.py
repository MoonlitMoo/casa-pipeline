# import pipeline.infrastructure.renderer.basetemplates as basetemplates
# import pipeline.infrastructure.pipelineqa as pipelineqa
# import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from .makecutoutimages import Makecutoutimages
from . import makecutoutimages
from . import renderer
# from . import qa

# pipelineqa.registry.add_handler(qa.MakecutoutimagesQAHandler())
# pipelineqa.registry.add_handler(qa.MakecutoutimagesListQAHandler())
# qaadapter.registry.register_to_dataset_topic(makecutoutimages.MakecutoutimagesResults)


def _get_imaging_mode(context, result):
    """Check the last selected imaging mode and pick the custom renderer for VLASS-SE-CUBE."""
    if hasattr(context, 'imaging_mode') and context.imaging_mode == 'VLASS-SE-CUBE':
        return 'VLASS-CUBE'
    else:
        return None


weblog.add_renderer(Makecutoutimages,
                    renderer.T2_4MDetailsMakecutoutimagesRenderer(description='Makecutoutimages'),
                    group_by=weblog.UNGROUPED)

# use the dedicated renderer and template for VLASS-SE-CUBE
weblog.add_renderer(Makecutoutimages,
                    renderer.T2_4MDetailsMakecutoutimagesVlassCubeRenderer(description='Makecutoutimages'),
                    group_by=weblog.UNGROUPED,
                    key='VLASS-CUBE',
                    key_fn=_get_imaging_mode)
