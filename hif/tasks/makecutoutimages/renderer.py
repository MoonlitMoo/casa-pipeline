import os

import numpy as np

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
from pipeline.infrastructure import casa_tools

from . import display

LOG = logging.get_logger(__name__)


class T2_4MDetailsMakecutoutimagesRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='makecutoutimages.mako',
                 description='Produce cutout images',
                 always_rerender=False):
        super().__init__(uri=uri,
                         description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        # There is only ever one MakermsimagesResults in the ResultsList as it
        # operates over multiple measurement sets, so we can set the result to
        # the first item in the list

        # Get results info
        info_dict = {}
        subplots = {}

        result = results[0]

        for subimagename in result.subimagenames:
            image_path = subimagename
            LOG.info('Getting properties of %s for the weblog.' % (image_path))

            with casa_tools.ImageReader(image_path) as image:
                info = image.miscinfo()
                spw = info.get('virtspw', None)
                field = ''
                coordsys = image.coordsys()
                coord_names = np.array(coordsys.names())
                coord_refs = coordsys.referencevalue(format='s')
                coordsys.done()
                pol = coord_refs['string'][coord_names == 'Stokes'][0]
                info_dict[(field, spw, pol, 'image name')] = image.name(strippath=True)

        image_size = result.image_size
        # Make the plots of the rms images
        plotter = display.CutoutimagesSummary(context, result)
        plots = plotter.plot()
        # PIPE-631: Weblog thumbnails are sorted according 'isalpha' parameter.
        for p in plots:
            if ".alpha" in p.basename:
                p.parameters['isalpha'] = 1
            else:
                p.parameters['isalpha'] = 0

        mslist_str = '<br>'.join([os.path.basename(vis) for vis in result.inputs['vis']])
        subplots[mslist_str] = plots

        ctx.update({'subplots': subplots,
                    'info_dict': info_dict,
                    'dirname': weblog_dir,
                    'plotter': plotter,
                    'image_size': image_size})


class T2_4MDetailsMakecutoutimagesVlassCubeRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlasscube_makecutoutimages.mako',
                 description='Produce cutout images',
                 always_rerender=False):
        super().__init__(uri=uri,
                         description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        # There is only ever one MakermsimagesResults in the ResultsList as it
        # operates over multiple measurement sets, so we can set the result to
        # the first item in the list

        # Get results info
        info_dict = {}
        img_plots = {}
        rms_plots = {}

        result = results[0]

        for sci_im in result.subimagelist:
            info_dict[sci_im['metadata']['spw']] = sci_im['metadata'].get('keep', True)

        for subimagename in result.subimagenames:
            image_path = subimagename
            LOG.info('Getting properties of %s for the weblog.' % (image_path))

            with casa_tools.ImageReader(image_path) as image:
                info = image.miscinfo()
                spw = info.get('virtspw', None)
                field = ''

                coordsys = image.coordsys()
                coord_names = np.array(coordsys.names())
                coord_refs = coordsys.referencevalue(format='s')
                coordsys.done()
                pol = coord_refs['string'][coord_names == 'Stokes'][0]
                info_dict[(field, spw, pol, 'image name')] = image.name(strippath=True)

        image_size = result.image_size

        # Make the RMS summary plots
        plotter = display.VlassCubeCutoutRmsSummary(context, result, info_dict)
        rms_plots['Cutout Rms Summary Plots'] = plotter.plot()

        # Make the plots of the cutout images
        plotter = display.VlassCubeCutoutimagesSummary(context, result)
        img_plots['Cutout Image Summary Plots'] = plotter.plot()

        # Export Stats summary
        stats_summary = display.get_stats_summary(result.stats)
        stats = result.stats

        # PIPE-631: Weblog thumbnails are sorted according 'isalpha' parameter.
        for p in img_plots['Cutout Image Summary Plots']:
            if ".alpha" in p.basename:
                p.parameters['isalpha'] = 1
            else:
                p.parameters['isalpha'] = 0

        ctx.update({'img_plots': img_plots,
                    'rms_plots': rms_plots,
                    'info_dict': info_dict,
                    'dirname': weblog_dir,
                    'stats': stats,
                    'stats_summary': stats_summary,
                    'image_size': image_size})
