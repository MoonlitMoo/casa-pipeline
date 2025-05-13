import collections
import copy
import os

import matplotlib

import pipeline.infrastructure as infrastructure
from pipeline.h.tasks.common.displays import sky as sky

LOG = infrastructure.get_logger(__name__)

# class used to transfer image statistics through to plotting routines
ImageStats = collections.namedtuple('ImageStats', 'rms max')


class MaskSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        # self.image_stats = image_stats

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.trace('Plotting')
        plot_wrappers = []

        # this class can handle a list of results, or a single
        # result if we make the single result from the latter
        # the member of a list
        if hasattr(self.result, 'results'):
            results = self.result.results
        else:
            results = [self.result]

        for r in results:
            # if r.empty():
            #    continue

            # extension = '.tt0' if r.multiterm else ''

            # mask map
            cmap = copy.copy(matplotlib.cm.binary)
            plot_wrappers.append(sky.SkyDisplay().plot(self.context, r.plotmask,
                                                       reportdir=stage_dir, intent='MASK',
                                                       collapseFunction='mean', cmap=cmap, dpi=900))

        return [p for p in plot_wrappers if p is not None]
