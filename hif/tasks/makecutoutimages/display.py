import collections
import os

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.pyplot import cm

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils.compatibility as compatibility
from pipeline.h.tasks.common.displays import sky
from pipeline.h.tasks.common.displays.imhist import ImageHistDisplay
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.displays.plotstyle import matplotlibrc_formal

LOG = infrastructure.get_logger(__name__)

# class used to transfer image statistics through to plotting routines
ImageStats = collections.namedtuple('ImageStats', 'rms max')


class CutoutimagesSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.info("Making PNG cutout images for weblog")
        plot_wrappers = []

        for subimagename in self.result.subimagenames:
            if '.psf.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean',
                                                           vmin=-0.1, vmax=0.3))
            elif '.image.' in subimagename and '.pbcor' not in subimagename:
                # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.image_stats = image.statistics(robust=True)

            elif '.residual.' in subimagename and '.pbcor.' not in subimagename:
                # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.residual_stats = image.statistics(robust=True)

            elif '.image.pbcor.' in subimagename and '.rms.' not in subimagename:
                # CAS-10345/PIPE-1189: use (-5*RMSmedian, 20*RMSmedian) as the colormap scaling range
                # We expect 'subimagename' here to be: X.image.pbcor.ttX.subim or X.image.pbcor.subim
                rms_subimagename = os.path.splitext(subimagename)[0]+'.rms.subim'
                with casa_tools.ImageReader(rms_subimagename) as image:
                    rms_median = image.statistics(robust=True).get('median')[0]
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean',
                                                           vmin=-5 * rms_median,
                                                           vmax=20 * rms_median))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pbcor_stats = image.statistics(robust=True)

            elif '.rms.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.rms_stats = image.statistics(robust=True)
                        arr = image.getchunk()
                        # PIPE-489 changed denominators to unmasked (non-zero) pixels
                        # get fraction of pixels <= 120 micro Jy VLASS technical goal.  ignore 0 (masked) values.
                        self.result.RMSfraction120 = (np.count_nonzero((arr != 0) & (arr <= 120e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # get fraction of pixels <= 168 micro Jy VLASS SE goal.  ignore 0 (masked) values.
                        self.result.RMSfraction168 = (np.count_nonzero((arr != 0) & (arr <= 168e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # get fraction of pixels <= 200 micro Jy VLASS technical requirement.  ignore 0 (masked) values.
                        self.result.RMSfraction200 = (np.count_nonzero((arr != 0) & (arr <= 200e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # PIPE-642: include the number and percentage of masked pixels in weblog
                        self.result.n_masked = np.count_nonzero(arr == 0)
                        self.result.pct_masked = (np.count_nonzero(arr == 0) / float(arr.size)) * 100

            elif '.residual.pbcor.' in subimagename and not subimagename.endswith('.rms'):
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pbcor_residual_stats = image.statistics(robust=True)

            elif '.pb.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean', vmin=0.2, vmax=1.))
                plot_wrappers.append(ImageHistDisplay(self.context, subimagename,
                                                      x_axis='Primary Beam Response', y_axis='Num. of Pixel',
                                                      reportdir=stage_dir).plot())
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pb_stats = image.statistics(robust=True)
            else:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))

        return [p for p in plot_wrappers if p is not None]


class VlassCubeCutoutimagesSummary(object):
    """A class for the VLASS-CUBE makecutout image summary plots."""

    def __init__(self, context, result):
        self.context = context
        self.result = result

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.info("Making PNG cutout images for weblog")
        plot_wrappers = []

        for subimagename in self.result.subimagenames:

            if '.psf.' in subimagename:
                plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                                      reportdir=stage_dir, intent='', stokes_list=['I'],
                                                                      collapseFunction='mean',
                                                                      vmin=-0.1, vmax=0.3))

            elif '.image.' in subimagename and '.pbcor' not in subimagename:
                # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                pass

            elif '.residual.' in subimagename and '.pbcor.' not in subimagename:
                # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                pass

            elif '.image.pbcor.' in subimagename and '.rms.' not in subimagename:
                # CAS-10345/PIPE-1189: use (-5*RMSmedian, 20*RMSmedian) as the colormap scaling range
                # We expect 'subimagename' here to be: X.image.pbcor.ttX.subim or X.image.pbcor.subim
                rms_subimagename = os.path.splitext(subimagename)[0]+'.rms.subim'
                with casa_tools.ImageReader(rms_subimagename) as image:
                    rms_median = image.statistics(robust=True).get('median')[0]
                plots = sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                         reportdir=stage_dir, intent='', stokes_list=None,
                                                         collapseFunction='mean',
                                                         vmin=-5 * rms_median,
                                                         vmax=20 * rms_median)
                for plot in plots:
                    plot.parameters['type'] = 'image.pbcor'
                plot_wrappers.extend(plots)

            elif '.rms.' in subimagename:
                plots = sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                         reportdir=stage_dir, intent='', stokes_list=None,
                                                         collapseFunction='mean')
                for plot in plots:
                    plot.parameters['type'] = 'image.pbcor.rms'
                plot_wrappers.extend(plots)

            elif '.residual.pbcor.' in subimagename and not subimagename.endswith('.rms'):
                plots = sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                         reportdir=stage_dir, intent='', stokes_list=None,
                                                         collapseFunction='mean')
                for plot in plots:
                    plot.parameters['type'] = 'residual.pbcor'
                plot_wrappers.extend(plots)

            elif '.pb.' in subimagename:
                plots = sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                         reportdir=stage_dir, intent='', stokes_list=['I'],
                                                         collapseFunction='mean', vmin=0.2, vmax=1.)
                for plot in plots:
                    plot.parameters['type'] = 'pb'
                plot_wrappers.extend(plots)
                plot_hist = ImageHistDisplay(self.context, subimagename,
                                             x_axis='Primary Beam Response', y_axis='Num. of Pixel',
                                             reportdir=stage_dir).plot()
                plot_hist.parameters['virtspw'] = plot_wrappers[-1].parameters['virtspw']
                plot_hist.parameters['band'] = plot_wrappers[-1].parameters['band']
                plot_hist.parameters['type'] = plot_wrappers[-1].parameters['type']
                plot_hist.parameters['stokes'] = 'I'
                plot_wrappers.append(plot_hist)
            else:
                plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                                      reportdir=stage_dir, intent='', stokes_list=None,
                                                                      collapseFunction='mean'))

        plot_wrappers = [p for p in plot_wrappers if p is not None]
        for p in plot_wrappers:
            p.parameters['order_idx'] = 10
            if p.parameters['type'] == 'image.pbcor':
                p.parameters['order_idx'] = 1
            if p.parameters['type'] == 'residual.pbcor':
                p.parameters['order_idx'] = 2
            if p.parameters['type'] == 'image.pbcor.rms':
                p.parameters['order_idx'] = 3
            if p.parameters['type'] == 'pb':
                p.parameters['order_idx'] = 4
            if p.parameters['type'] == 'psf':
                p.parameters['order_idx'] = 4

        return plot_wrappers


def get_stats_summary(stats):
    """Reorganize the raw stats (spw,imtype) into a layout (imtype,property) more convenient for tabulation."""

    stats_summary = collections.OrderedDict()
    for spw, stats_spw in stats.items():
        for imtype, stats_spw_imtype in stats_spw.items():
            if imtype in ['stokes', 'reffreq']:
                # deal with two special keys
                continue
            for item, value in stats_spw_imtype.items():
                if imtype not in stats_summary:
                    stats_summary[imtype] = collections.OrderedDict()
                if item not in stats_summary[imtype]:
                    stats_summary[imtype][item] = {'spw': [], 'value': []}
                stats_summary[imtype][item]['spw'].append(spw)
                stats_summary[imtype][item]['value'].append(value)

    for imtype, stats_summary_imtype in stats_summary.items():
        for item, item_details in stats_summary_imtype.items():
            value_arr = np.abs(np.array(item_details['value']))         # shape=(n_spw, n_pol)
            spw_arr = np.array(item_details['spw'])                     # shape=(n_spw,)
            # note: np.stats.median_absolute_deviation has the default scale=1.4826 and is deprecated on scipy ver>1.5.0.
            # TODO: It should be replaced with scipy.stats.median_abs_deviation(x, scale='normal') in the future.
            mad_func = compatibility.get_scipy_function_for_mad()
            stats_summary[imtype][item]['spwwise_madrms'] = mad_func(value_arr, axis=0)
            stats_summary[imtype][item]['spwwise_median'] = np.median(value_arr, axis=0)
            stats_summary[imtype][item]['range'] = np.percentile(value_arr, (0, 100))
            idx_maxdev = np.argmax(value_arr-np.median(value_arr, axis=0), axis=0)
            stats_summary[imtype][item]['spw_outlier'] = spw_arr[idx_maxdev]

    return stats_summary


class VlassCubeCutoutRmsSummary(object):
    """A class for the VLASS-CUBE makecutout rms-vs-frequency summary plots."""

    def __init__(self, context, result, info_dict=None):
        self.context = context
        self.result = result
        self.info_dict = info_dict

    @matplotlibrc_formal
    def plot(self, improp_list=None):

        if improp_list is None:
            improp_list = [('image', 'MADrms'), ('rms', 'Median')]
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        plots = []
        for improp in improp_list:
            figfile = os.path.join(stage_dir, f'{improp[0]}_{improp[1]}_vs_freq.png')
            plot = self._plot_rms(figfile=figfile, imtype=improp[0], item=improp[1])
            plots.append(plot)

        return [p for p in plots if p is not None]

    def _plot_rms(self, figfile, imtype='rms', item='median'):

        LOG.debug(f'Creating the {imtype}_{item} vs. Frequency plot.')

        try:

            stats = self.result.stats
            x = []
            y = []
            spw_labels = []
            for spw in stats:
                if imtype.lower() in stats[spw]:
                    x.append(stats[spw]['reffreq'])
                    y.append(stats[spw][imtype.lower()][item.lower()])
                    spw_labels.append(spw)
                    stokes_list = stats[spw]['stokes']
            x = np.array(x)/1e9  # GHz
            y = np.array(y)*1e3  # mJy
            spw_labels = np.array(spw_labels)

            fig, ax = plt.subplots(figsize=(8, 6))
            cmap = cm.get_cmap('rainbow_r')
            if isinstance(self.info_dict, dict):
                keep_list = [self.info_dict.get(spw_label, True) for spw_label in spw_labels]
            else:
                keep_list = [True]*len(spw_labels)
            for idx, stokes in enumerate(stokes_list):
                color_idx = idx/len(stokes_list)

                ax.scatter(x, y[:, idx], facecolors='none', alpha=0.75, s=12**2, edgecolors='black')
                ax.plot(x[keep_list], y[keep_list, idx], marker='o', markeredgecolor=cmap(color_idx),
                        markerfacecolor=cmap(color_idx), markersize=12, label=fr'$\it {stokes}$', alpha=0.75)

                for idx_spw in range(len(x)):
                    if stokes == 'I':
                        text = ax.annotate(spw_labels[idx_spw], (x[idx_spw], y[idx_spw, idx]),
                                           ha='center', va='center', fontsize=8.)
                        text.set_alpha(.7)

            ax.set_xlabel('Frequency [GHz]')
            ax.set_ylabel(f'{imtype}'+r'$_{\rm '+f'{item}'+'}$ [mJy/beam]')
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            ax.set_xlim(1.9, 4.1)
            ax.set_ylim(ylim)

            # 'RflagDevHeuristic' is only imported on-demand; if it's imported during module initialization,
            # a circular import will introduce problem (hif->hifv->hif)
            from pipeline.hifv.heuristics.rfi import RflagDevHeuristic
            sefd = RflagDevHeuristic.get_vla_sefd()

            for band, sefd_per_band in sefd.items():
                sefd_x = sefd_per_band[:, 0]/1e3
                sefd_y = sefd_per_band[:, 1]
                if np.min(sefd_x) < np.mean(x) < np.max(sefd_x):
                    LOG.info(f'Selecting Band {band} for the SEFD-based rms prediction.')
                    sefd_spw = np.interp(x, sefd_x, sefd_y)
                    scale = np.median(np.divide(y[:, 1:], sefd_spw[:, np.newaxis]))
                    ax.plot(sefd_x, sefd_y*scale, color='gray', label=r'SEFD$_{\rm norm}$', linestyle='-')

            # ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            ax.legend(fontsize=12, labelspacing=0.5)

            fig.tight_layout()
            fig.savefig(figfile)
            plt.close(fig)

            plot = logger.Plot(figfile,
                               x_axis='Frequency',
                               y_axis=item+'<sub>.'+imtype+'</sub>',
                               parameters={})

        except Exception as ex:
           LOG.warning("Could not create plot {}".format(figfile))
           LOG.warning(ex)
           plot = None

        return plot
