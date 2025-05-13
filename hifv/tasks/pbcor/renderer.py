import os

import numpy
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
from pipeline.infrastructure import casa_tools

from . import display as pbcorimages

LOG = logging.get_logger(__name__)


class T2_4MDetailsMakepbcorimagesRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='pbcor.mako',
                 description='Produce primary beam corrected tt0 images',
                 always_rerender=False):
        super().__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        # There is only ever one CleanListResult in the ResultsList as it
        # operates over multiple measurement sets, so we can set the result to
        # the first item in the list
        if not results[0]:
            return

        # Get results info
        info_dict = {}
        pbcorplots = {}

        pbcor_result = results[0]

        pbcor_dict = pbcor_result.pbcorimagenames
        info_dict['multitermlist'] = pbcor_result.multitermlist

        # Make the plots of the pbcor images
        plotter = pbcorimages.PbcorimagesSummary(context, pbcor_result)
        plot_dict = plotter.plot()

        mslist_str = '<br>'.join([os.path.basename(vis) for vis in pbcor_result.inputs['vis']])
        pbcorplots[mslist_str] = plot_dict

        for basename_keep, pbcor_images in pbcor_dict.items():
            basename = basename_keep[0]
            keep = basename_keep[1]
            info_dict[basename] = keep
            for image_path in pbcor_images:
                LOG.info('Getting properties of %s for the weblog.' % image_path)

                with casa_tools.ImageReader(image_path) as image:
                    info = image.miscinfo()
                    spw = info.get('virtspw', None)
                    field = ''
                    # if 'field' in info:
                    #     field = '%s (%s)' % (info['field'], r.intent)
                    coordsys = image.coordsys()
                    coord_names = numpy.array(coordsys.names())
                    coord_refs = coordsys.referencevalue(format='s')
                    coordsys.done()
                    pol = coord_refs['string'][coord_names == 'Stokes'][0]
                    info_dict[(field, spw, pol, 'image name')] = image.name(strippath=True)

        ctx.update({'pbcorplots': pbcorplots,
                    'info_dict': info_dict,
                    'dirname': weblog_dir,
                    'plotter': plotter})
