import os
import numpy as np

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
from pipeline.infrastructure import casa_tools

from . import display as rmsimages

LOG = logging.get_logger(__name__)


class T2_4MDetailsMakermsimagesRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='makermsimages.mako',
                 description='Produce rms images',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        # There is only ever one MakermsimagesResults in the ResultsList as it
        # operates over multiple measurement sets, so we can set the result to
        # the first item in the list

        # Get results info
        info_dict = {}
        rmsplots = {}

        result = results[0]

        for rmsimagename in result.rmsimagenames:
            image_path = rmsimagename
            LOG.info('Getting properties of %s for the weblog.' % (image_path))

            with casa_tools.ImageReader(image_path) as image:
                info = image.miscinfo()
                spw = info.get('virtspw', None)
                field = ''
                #if 'field' in info:
                #    field = '%s (%s)' % (info['field'], r.intent)

                coordsys = image.coordsys()
                coord_names = np.array(coordsys.names())
                coord_refs = coordsys.referencevalue(format='s')
                coordsys.done()
                pol = coord_refs['string'][coord_names == 'Stokes'][0]
                info_dict[(field, spw, pol, 'image name')] = image.name(strippath=True)

        # Make the plots of the rms images
        plotter = rmsimages.RmsimagesSummary(context, result)
        plots = plotter.plot()
        mslist_str = '<br>'.join([os.path.basename(vis) for vis in result.inputs['vis']])
        rmsplots[mslist_str] = plots

        ctx.update({'rmsplots': rmsplots,
                    'info_dict': info_dict,
                    'dirname': weblog_dir,
                    'plotter': plotter})


class T2_4MDetailsMakermsimagesVlassCubeRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlasscube_makermsimages.mako',
                 description='Produce rms images',
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
        result = results[0]

        rmsimagenames = result.rmsimagenames
        for sci_im in result.rmsimagelist:
            info_dict[sci_im['metadata']['spw']] = sci_im['metadata'].get('keep', True)

        for rmsimagename in rmsimagenames:
            image_path = rmsimagename
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

        # Make the plots of the rms images
        plotter = rmsimages.VlassCubeRmsimagesSummary(context, result)
        rmsplots = {'Rms Image Summary Plots': plotter.plot()}

        ctx.update({'rmsplots': rmsplots,
                    'info_dict': info_dict,
                    'dirname': weblog_dir,
                    'plotter': plotter})
