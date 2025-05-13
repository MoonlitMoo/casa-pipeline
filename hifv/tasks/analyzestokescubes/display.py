import os

import matplotlib.pyplot as plt
import numpy as np
from astropy.modeling import fitting, models
from matplotlib.pyplot import cm
from matplotlib.ticker import ScalarFormatter

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.infrastructure.displays.plotstyle import matplotlibrc_formal

LOG = infrastructure.get_logger(__name__)


class VlassCubeStokesSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

    @matplotlibrc_formal
    def plot(self):

        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        plot_wrappers = []
        stats = self.result.stats

        for roi_name, roi_stats in stats.items():
            figfile = os.path.join(stage_dir, f'stokes_summary_u_vs_q_{roi_name}.png')

            LOG.debug(f'Creating the ROI={roi_name} Stokes U vs. Q plot.')

            try:
                x = np.array(roi_stats['stokesq'])/np.array(roi_stats['stokesi'])
                y = np.array(roi_stats['stokesu'])/np.array(roi_stats['stokesi'])
                label_spw = roi_stats['spw']
                label_full = []
                for idx, reffreq in enumerate(roi_stats['reffreq']):
                    label_str = roi_stats['spw'][idx]
                    label_str += ' : '
                    label_str += f'{reffreq/1e9:.3f} GHz'
                    if not roi_stats['keep'][idx]:
                        label_str += ' (rejected)'
                    label_full.append(label_str)

                fig, ax = plt.subplots(figsize=(10, 7))
                cmap = cm.get_cmap('rainbow_r')
                for idx in range(len(x)):
                    color_idx = idx/len(x)
                    if 'rejected' in label_full[idx]:
                        facecolors = 'none'
                    else:
                        facecolors = cmap(color_idx)
                    ax.scatter(x[idx], y[idx], facecolors=facecolors,
                               label=label_full[idx], edgecolors='black', alpha=0.7, s=300.)
                    text = ax.annotate(label_spw[idx], (x[idx], y[idx]), ha='center', va='center', fontsize=9.)
                    text.set_alpha(.7)

                ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize=12, labelspacing=0.75)
                ax.set_xlabel('Frac. Stokes $Q$')
                ax.set_ylabel('Frac. Stokes $U$')
                peak_loc = roi_stats['world']
                peak_loc_xy = 'Pix Loc.: '+str(roi_stats['xy'])

                desc = None
                if roi_name == 'peak_stokesi':
                    desc = 'Peak of the Stokes-I map at {:.3f} GHz'.format(min(roi_stats['reffreq'])/1e9)
                if roi_name == 'peak_linpolint':
                    desc = 'Peak of the linearly polarized intensity map at {:.3f} GHz'.format(
                        min(roi_stats['reffreq'])/1e9)

                ax.set_title(f"{peak_loc}\n{peak_loc_xy}")
                ax.set_aspect('equal')
                ax.axhline(0, linestyle='-', color='lightgray')
                ax.axvline(0, linestyle='-', color='lightgray')

                amp_max = np.max(np.abs(np.array([ax.get_xlim(), ax.get_ylim()])))
                amp_scale = 1.2
                ax.set_xlim(-amp_max*amp_scale, amp_max*amp_scale)
                ax.set_ylim(-amp_max*amp_scale, amp_max*amp_scale)

                fig.savefig(figfile)

                plt.close(fig)

                plot = logger.Plot(figfile,
                                   x_axis='Frac. Stokes-Q',
                                   y_axis='Frac. Stokes-U',
                                   parameters={'desc': desc})
                plot_wrappers.append(plot)

            except Exception as ex:
                LOG.warning("Could not create plot {}".format(figfile))
                LOG.warning(ex)

        return plot_wrappers


class VlassCubeFluxSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

    @matplotlibrc_formal
    def plot(self):

        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        plot_wrappers = []
        stats = self.result.stats

        for roi_name, roi_stats in stats.items():
            figfile = os.path.join(stage_dir, f'stokes_summary_flux_vs_freq_{roi_name}.png')

            LOG.debug(f'Creating the ROI={roi_name} Flux vs. Freq plot.')

            try:

                y = np.array(roi_stats['stokesi'])*1e3
                weights = np.zeros(y.size)

                idx_keep = np.where(roi_stats['keep'])
                weights[idx_keep] = 1.0
                x = np.array(roi_stats['reffreq'])/1e9
                y_rms = np.array(roi_stats['rms'])[:, 0]*1e3

                fig, ax = plt.subplots(figsize=(10, 7))
                ax.scatter(x, y, label='Observed (rejected)', color='black', facecolors='none',s=10**2)
                ax.scatter(x[idx_keep], y[idx_keep], label='Observed', color='black',s=10**2)

                ax.set_xlabel('Freq [GHz]')
                ax.set_ylabel('Flux [mJy/beam]')

                xticks = ax.get_xticks()
                xticks_minor = ax.get_xticks(minor=True)
                xrange = ax.get_xlim()

                ax.set_xscale("log")
                ax.set_xticks(xticks)
                ax.set_xticks(xticks_minor, minor=True)
                ax.set_xlim(xmin=xrange[0], xmax=xrange[1])

                yticks = ax.get_yticks()
                yticks_minor = ax.get_yticks(minor=True)
                yrange = ax.get_ylim()

                ax.set_yscale("log")
                ax.set_yticks(yticks)
                ax.set_yticks(yticks_minor, minor=True)
                ax.set_ylim(ymin=yrange[0], ymax=yrange[1])

                for axis in [ax.xaxis, ax.yaxis]:
                    axis.set_major_formatter(ScalarFormatter())
                    axis.set_minor_formatter(ScalarFormatter())

                peak_loc = roi_stats['world']
                peak_loc_xy = 'Pix Loc.: '+str(roi_stats['xy'])

                desc = None
                if roi_name == 'peak_stokesi':
                    desc = 'Peak of the Stokes-I map at {:.3f} GHz'.format(min(roi_stats['reffreq'])/1e9)
                if roi_name == 'peak_linpolint':
                    desc = 'Peak of the linearly polarized intensity map at {:.3f} GHz'.format(
                        min(roi_stats['reffreq'])/1e9)

                snr_lim = 2.0
                ax.fill_between(x, yrange[0], y_rms*snr_lim, facecolor='lightgray',
                                alpha=0.5, label=r'Below $2\sigma$')

                ax.set_title(f"{peak_loc}\n{peak_loc_xy}")
                spec_model_fitted = self._model_powerlaw1d(x, y, weights=weights)
                x_m = np.linspace(2, 4, num=100)

                amplitude = spec_model_fitted.amplitude.value
                alpha = -spec_model_fitted.alpha.value
                label = r'$I_{\rm 3GHz}$ = '+f'{amplitude:.3f}'+' mJy/bm, '+r'$\alpha$='+f'{alpha:.3f}'
                label_text = r'I(3GHz) = '+f'{amplitude:.3f}'+' mJy/bm, '+r'alpha='+f'{alpha:.3f}'
                ax.plot(x_m, spec_model_fitted(x_m), label=label, color='red')
                LOG.info(f'PowerLaw1D Fitting Result for ROI={roi_name}: {label_text}')

                model_flux = spec_model_fitted(x)/1e3  # in Jy/beam
                ax.legend()

                fig.savefig(figfile)
                plt.close(fig)

                plot = logger.Plot(figfile,
                                   x_axis='Freq',
                                   y_axis='Flux',
                                   parameters={'desc': desc,
                                               'roi_name': roi_name,
                                               'model_amplitude': amplitude/1e3,  # Jy/bm
                                               'model_alpha': alpha,
                                               'model_label': label,
                                               'model_flux': model_flux})         # Jy/bm
                plot_wrappers.append(plot)

            except Exception as ex:
                LOG.warning("Could not create plot {}".format(figfile))
                LOG.warning(ex)

        return plot_wrappers

    def _model_powerlaw1d(self, freq, flux, weights=None):

        fit = fitting.LevMarLSQFitter()
        spec_model = models.PowerLaw1D(x_0=3.0)
        spec_model.amplitude.min = 0
        spec_model.x_0.fixed = True
        spec_model_fitted = fit(spec_model, freq, flux, weights=weights)
        LOG.debug(str(spec_model_fitted))

        return spec_model_fitted
