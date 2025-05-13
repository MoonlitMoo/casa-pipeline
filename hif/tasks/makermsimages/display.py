import collections
import os
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils.compatibility as compatibility
from pipeline.h.tasks.common.displays import sky as sky
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

# class used to transfer image statistics through to plotting routines
ImageStats = collections.namedtuple('ImageStats', 'rms max')


class RmsimagesSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        # self.image_stats = image_stats

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.info("Making PNG RMS images for weblog")
        plot_wrappers = []
        for rmsimagename in self.result.rmsimagenames:
            plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context, rmsimagename,
                                                                  reportdir=stage_dir, intent='', stokes_list=None,
                                                                  collapseFunction='mean'))
            # PIPE-1163: avoid saving stats from .tt1
            if '.tt1.' not in rmsimagename:
                with casa_tools.ImageReader(rmsimagename) as image:
                    stats = image.statistics(robust=True)
                    self.result.max = stats.get('max')[0]
                    self.result.min = stats.get('min')[0]
                    self.result.mean = stats.get('mean')[0]
                    self.result.median = stats.get('median')[0]
                    self.result.sigma = stats.get('sigma')[0]
                    self.result.MADrms = stats.get('medabsdevmed')[0] * 1.4826  # see CAS-9631

        return [p for p in plot_wrappers if p is not None]


class VlassCubeRmsimagesSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.result.stats = []

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.info("Making PNG RMS images for weblog")
        plot_wrappers = []
        for rmsimagename in self.result.rmsimagenames:
            plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context, rmsimagename,
                                                                  reportdir=stage_dir, intent='', stokes_list=None,
                                                                  collapseFunction='mean'))

            with casa_tools.ImageReader(rmsimagename) as image:
                stats = image.statistics(robust=True, axes=[0, 1, 3])
                stats['virtspw'] = plot_wrappers[-1].parameters['virtspw']
                stats['madrms'] = stats.get('medabsdevmed') * 1.4826  # see CAS-9631
                self.result.stats.append(stats)

        stats_summary = {}
        for item in ['max', 'min', 'mean', 'median', 'sigma', 'madrms']:
            stats_summary[item] = {'range': np.percentile([stats[item] for stats in self.result.stats], (0, 100))}
            value_arr = np.array([stats[item] for stats in self.result.stats])
            # note: np.stats.median_absolute_deviation has the default scale=1.4826 and is deprecated with scipy>1.5.0.
            # TODO: It should replaced with scipy.stats.median_abs_deviation(x, scale='normal') in the future.
            mad_func = compatibility.get_scipy_function_for_mad()
            stats_summary[item]['spwwise_madrms'] = mad_func(value_arr, axis=0)
            stats_summary[item]['spwwise_median'] = np.median(value_arr, axis=0)
        self.result.stats_summary = stats_summary

        return [p for p in plot_wrappers if p is not None]
