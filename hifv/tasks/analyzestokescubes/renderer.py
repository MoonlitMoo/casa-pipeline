import os
import copy
import numpy as np

from . import display as analyzestokescube
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates

LOG = logging.get_logger(__name__)


class T2_4MDetailsAnalyzestokesCubeRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlasscube_makermsimages.mako',
                 description='Produce rms images',
                 always_rerender=False):
        super().__init__(uri=uri,
                         description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        # Get results info (only one)
        result = results[0]

        # Make the Stokes Q vs. U plot
        stokesplots = {'Stokes Summary Plots': analyzestokescube.VlassCubeStokesSummary(context, result).plot()}

        # Make the Sokes I vs. Freq plot
        fluxplots = {'Flux vs. Freq Plots': analyzestokescube.VlassCubeFluxSummary(context, result).plot()}

        stats = result.stats
        for idx, (roi_name, roi_stats) in enumerate(stats.items()):
            for key in ['model_flux', 'model_amplitude', 'model_alpha']:
                try:
                    roi_stats[key] = fluxplots['Flux vs. Freq Plots'][idx].parameters[key]
                except Exception as e:
                    if key in ('model_alpha', 'model_amplitude'):
                        roi_stats[key] = np.nan
                    else:
                        roi_stats[key] = np.nan*np.array(roi_stats['stokesi'])

        # Make the rms vs. freq plot by reusing the result from hif_makecutoutimages
        try:

            from pipeline.hif.tasks.makecutoutimages.display import VlassCubeCutoutRmsSummary
            results_list = context.results

            if results_list and type(results_list) is list:
                for result in results_list:
                    result_meta = result
                    if hasattr(result_meta, 'pipeline_casa_task') and result_meta.pipeline_casa_task.startswith(
                            'hif_makecutoutimages'):
                        r_makecutoutimages_copy = copy.deepcopy(result_meta)
            r_makecutoutimages_copy.stage_number = result.stage_number
            info_dict = {}
            for sci_im in r_makecutoutimages_copy.subimagelist:
                info_dict[sci_im['metadata']['spw']] = sci_im['metadata'].get('keep', True)
            plotter = VlassCubeCutoutRmsSummary(context, r_makecutoutimages_copy, info_dict)
            rmsplots = {'Rms Summary Plot': plotter.plot(improp_list=[('rms', 'Median')])}

        except Exception as e:
            rmsplots = {'Rms Summary Plot': []}

        ctx.update({'rmsplots': rmsplots,
                    'stokesplots': stokesplots,
                    'fluxplots': fluxplots,
                    'stats': stats,
                    'dirname': weblog_dir})
