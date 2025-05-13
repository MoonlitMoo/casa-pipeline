import collections
import copy
import os

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.h.tasks.common.displays import sky as sky
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils import get_stokes

from .plot_beams import plot_beams
from .plot_beams_vlasscube import plot_beams_vlasscube
from .plot_spectra import plot_spectra

LOG = infrastructure.get_logger(__name__)

# class used to transfer image statistics through to plotting routines
ImageStats = collections.namedtuple('ImageStats', 'rms max')


class CleanSummary(object):
    def __init__(self, context, result, image_stats):
        self.context = context
        self.result = result
        self.image_stats = image_stats

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.trace('Plotting')
        plot_wrappers = []

        # this class can handle a list of results from hif_cleanlist, or a single
        # result from hif_clean if we make the single result from the latter
        # the member of a list
        if hasattr(self.result, 'results'):
            results = self.result.results
        else:
            results = [self.result]

        for r in results:
            if r.empty():
                continue

            extension = '.tt0' if r.multiterm else ''

            # PIPE-1401: use the same selecting list constructed from the Stokes plane present in the PSF image.
            stokes_list = get_stokes(r.psf + extension)

            # psf map
            plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context, r.psf + extension,
                                                                  reportdir=stage_dir, intent=r.intent, stokes_list=stokes_list,
                                                                  collapseFunction='mean'))

            # flux map
            plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context,
                                                            r.flux + extension, reportdir=stage_dir, intent=r.intent, stokes_list=stokes_list,
                                                            collapseFunction='mean'))

            for i, iteration in [(k, r.iterations[k]) for k in sorted(r.iterations)]:

                # process image for this iteration
                if 'image' in iteration:

                    collapse_function = 'mom8' if self._is_cube_repbw(iteration) else 'mean'
                    image_path = iteration['image'].replace('.image', '.image%s' % (extension))

                    # PB corrected
                    plot_wrappers.extend(
                        sky.SkyDisplay().plot_per_stokes(self.context, image_path, reportdir=stage_dir, intent=r.intent, stokes_list=stokes_list,
                                                         collapseFunction=collapse_function))
                    # For cubes (i.e. when mom8 is selected above) also make mom0 of the PB corrected image (PIPE-652)
                    if collapse_function == 'mom8':
                        plot_wrappers.extend(
                            sky.SkyDisplay().plot_per_stokes(self.context, image_path, reportdir=stage_dir, intent=r.intent, stokes_list=stokes_list,
                                                             collapseFunction='mom0'))

                    # Non PB corrected
                    image_path = image_path.replace('.pbcor', '')

                    # CAS-8847: For the non-pbcor MOM8 image displayed in the weblog set the color range to: +1
                    # sigmaCube to max([peakCube,+8sigmaCube]). The pbcor MOM8 images are ok as is for now.
                    extra_args = {}
                    if collapse_function == 'mom8':
                        if image_path not in self.image_stats:
                            LOG.trace('No cached image statistics found for {!s}'.format(image_path))
                            with casa_tools.ImageReader(image_path) as image:
                                stats = image.statistics(robust=False)
                                image_rms = stats.get('rms')[0]
                                image_max = stats.get('max')[0]
                                self.image_stats[image_path] = ImageStats(rms=image_rms, max=image_max)

                        image_stats = self.image_stats[image_path]
                        image_rms = image_stats.rms
                        image_max = image_stats.max

                        extra_args = {
                            'vmin': image_rms,
                            'vmax': max([image_max, 8*image_rms])
                        }

                    plot_wrappers.extend(
                        sky.SkyDisplay().plot_per_stokes(self.context, image_path, reportdir=stage_dir, intent=r.intent, stokes_list=stokes_list,
                                                   collapseFunction=collapse_function, **extra_args))

                # residual for this iteration
                # PIPE-652: display mom8 and mom8 for residual cubes
                residual_collapseFunction = ['mom8', 'mom0'] if self._is_cube_repbw(iteration, imagetype='residual') else ['mean']
                for collapseFunction in residual_collapseFunction:
                    plot_wrappers.extend(
                        sky.SkyDisplay().plot_per_stokes(self.context, iteration['residual'] + extension, reportdir=stage_dir,
                                                         intent=r.intent, stokes_list=stokes_list, collapseFunction=collapseFunction))

                # model for this iteration (currently only last but allow for others in future)
                if 'model' in iteration and os.path.exists(iteration['model'] + extension):
                    plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(
                        self.context, iteration['model'] + extension, reportdir=stage_dir, intent=r.intent,
                        stokes_list=stokes_list, **{'cmap': copy.copy(matplotlib.cm.seismic)}))

                # MOM0_FC for this iteration (currently only last but allow for others in future).
                if 'mom0_fc' in iteration and os.path.exists(iteration['mom0_fc'] + extension):
                    plot_wrappers.extend(
                        sky.SkyDisplay().plot_per_stokes(self.context, iteration['mom0_fc'] + extension, reportdir=stage_dir,
                                                         intent=r.intent, stokes_list=stokes_list))

                # MOM8_FC for this iteration (currently only last but allow for others in future).
                if 'mom8_fc' in iteration and os.path.exists(iteration['mom8_fc'] + extension):
                    # PIPE-197: For MOM8_FC image displayed in the weblog set the color range
                    # from (median-MAD) to 10 * sigma, where sigma is from the annulus minus cleanmask
                    # masked mom8_fc image and median, MAD are from the unmasked mom8_fc image
                    if iteration['cube_sigma_fc_chans'] is not None:
                        extra_args = {
                            'vmin': iteration['mom8_fc_image_median_all'] - iteration['mom8_fc_image_mad'],
                            'vmax': 10 * iteration['cube_sigma_fc_chans'],
                            'mom8_fc_peak_snr': iteration['mom8_fc_peak_snr']
                        }
                        # in case min >= max, set min=image_min and max=image_max
                        if extra_args['vmin'] >= extra_args['vmax']:
                            extra_args['vmin'] = iteration['mom8_fc_image_min']
                            extra_args['vmax'] = iteration['mom8_fc_image_max']
                    else:
                        extra_args = {}

                    plot_wrappers.extend(
                        sky.SkyDisplay().plot_per_stokes(self.context, iteration['mom8_fc'] + extension, reportdir=stage_dir,
                                                         intent=r.intent, stokes_list=stokes_list, **extra_args))

                # cleanmask - not for iter 0
                if i > 0:
                    collapse_function = 'mom8' if self._is_cube_repbw(iteration, imagetype='cleanmask') else 'mean'
                    plot_wrappers.extend(
                        sky.SkyDisplay().plot_per_stokes(self.context, iteration.get('cleanmask', ''), reportdir=stage_dir,
                                                         intent=r.intent, stokes_list=stokes_list, collapseFunction=collapse_function,
                                                         **{'cmap': copy.copy(matplotlib.cm.YlOrRd)}))

                # cube spectra and PSF per channel plot for this iteration
                if self._is_cube_repbw(iteration):
                    imagename = r.image_robust_rms_and_spectra['nonpbcor_imagename']
                    with casa_tools.ImageReader(imagename) as image:
                        miscinfo = image.miscinfo()

                    parameters = {k: miscinfo[k] for k in ['virtspw', 'iter'] if k in miscinfo}
                    parameters['field'] = '%s (%s)' % (miscinfo['field'], miscinfo['intent'])
                    parameters['datatype'] = r.datatype
                    parameters['type'] = 'spectra'
                    parameters['stokes'] = stokes_list[0]
                    parameters['moment'] = 'N/A'
                    try:
                        parameters['prefix'] = os.path.basename(imagename).split('.')[0]
                    except:
                        parameters['prefix'] = None

                    virtual_spw = parameters['virtspw']
                    ref_ms = self.context.observing_run.measurement_sets[0]
                    real_spw = self.context.observing_run.virtual2real_spw_id(virtual_spw, ref_ms)
                    real_spw_obj = ref_ms.get_spectral_window(real_spw)
                    if real_spw_obj.receiver is not None and real_spw_obj.freq_lo is not None:
                        rec_info = {'type': real_spw_obj.receiver, 'LO1': real_spw_obj.freq_lo[0].str_to_precision(12)}
                    else:
                        LOG.warning('Could not determine receiver type. Assuming TSB.')
                        rec_info = {'type': 'TSB', 'LO1': '0GHz'}

                    # Diagnostic spectra
                    plotfile = '%s.spectrum.png' % (os.path.join(stage_dir, os.path.basename(imagename)))
                    field_id = int(r.field_ids[0].split(',')[0])
                    plot_spectra(r.image_robust_rms_and_spectra, rec_info, plotfile, ref_ms.name, str(real_spw), field_id)
                    plot_wrappers.append(logger.Plot(plotfile, parameters=parameters))

                    # PSF per channels plot
                    psf_per_channel_plotfile = '%s.beams.png' % (os.path.join(stage_dir, os.path.basename(r.psf)))
                    plot_beams(r.psf, psf_per_channel_plotfile)
                    psf_per_channel_parameters = copy.deepcopy(parameters)
                    psf_per_channel_parameters['type'] = 'psf_per_channel'
                    psf_per_channel_parameters['stokes'] = stokes_list[0]
                    plot_wrappers.append(logger.Plot(psf_per_channel_plotfile, parameters=psf_per_channel_parameters))

            # polarization intensity and angle
            if r.intent == 'POLARIZATION' and set(stokes_list) == {'I', 'Q', 'U', 'V'} and r.imaging_mode == 'ALMA':
                plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context,
                                                                      r.image.replace('.pbcor', '').replace('IQUV', 'POLI'),
                                                                      reportdir=stage_dir, intent=r.intent,
                                                                      collapseFunction='mean'))
                plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context,
                                                                      r.image.replace('.pbcor', '').replace('IQUV', 'POLA'),
                                                                      reportdir=stage_dir, intent=r.intent,
                                                                      collapseFunction='mean'))

        return [p for p in plot_wrappers if p is not None]

    def _is_cube_repbw(self, iteration, imagetype='image'):
        is_cube_or_repbw = any(keyword in iteration.get(imagetype, '') for keyword in ['cube', 'repBW'])
        return is_cube_or_repbw

class TcleanMajorCycleSummaryFigure(object):
    """Tclean major cycle summery statistics plot with two panels, contains:

    Flux density cleaned vs. major cycle
    Plot of Peak Residual per major cycle

    Note that as of 04.2021 no clear tclean return dictionary was available and the unit
    of the flux and RMS was determined empirically.

    See PIPE-991."""

    def __init__(self, context, result, major_cycle_stats, figname='major_cycle_stats'):
        self.context = context
        self.majorcycle_stats = major_cycle_stats
        self.reportdir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
        self.figname = figname
        self.figfile = self._get_figfile()
        self.units = ['Jy', 'Jy/pixel']
        self.title = 'Major cycle statistics'
        self.xlabel = 'Minor iterations done'
        self.ylabel = ['Flux density cleaned (abs) [%s]' % self.units[0],
                       'Peak residual (abs) [%s]' % self.units[1]]
        self.unitfactor = [1.0, 1.0]
        self.pol_labels = list(result.targets[0]['stokes'])

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing tclean major cycle summary plot')
            return self._get_plot_object()

        LOG.info('Creating major cycle statistics plot.')

        fig, (ax0, ax1) = plt.subplots(2, 1, sharex=True)
        fig.set_dpi(150.0)

        ax0.set_title(self.title, fontsize=10)
        ax1.set_xlabel(self.xlabel, fontsize=8)
        ax0.set_ylabel(self.ylabel[0], fontsize=8)
        ax1.set_ylabel(self.ylabel[1], fontsize=8)

        ax0.tick_params(axis='both', which='both', labelsize=8)
        ax1.tick_params(axis='both', which='both', labelsize=8)

        use_ylog = True
        x0 = 0
        for iter, item in self.majorcycle_stats.items():
            if item['nminordone_array'] is not None:
                # get quantities
                x = item['nminordone_array'] + x0
                ax0_y = item['totalflux_array'] * self.unitfactor[0]
                ax1_y = item['peakresidual_array'] * self.unitfactor[1]

                # increment last iteration
                x0 = x[-1]

                # scatter plot
                pol_colors = ['gray', 'blue', 'green', 'red']
                pol_markers = ['+', '<', '>', 'o']
                pol_edgecolors = [None, 'black', 'black', 'black']
                pol_alphas = [1, 0.3, 0.3, 0.3]
                pol_id_arr = item['planeid_array']
                for pol_id in np.unique(pol_id_arr):

                    pid = int(pol_id)
                    ax0_pol_idx = np.where((pol_id_arr == pol_id) & (ax0_y != 0))
                    if ax0_pol_idx[0].size > 0:
                        ax0.scatter(x[ax0_pol_idx], np.abs(ax0_y[ax0_pol_idx]), label=self.pol_labels[pid], edgecolors=pol_edgecolors[pid],
                                    alpha=pol_alphas[pid], color=pol_colors[pid], marker=pol_markers[pid])
                    ax1_pol_idx = np.where((pol_id_arr == pol_id) & (ax1_y != 0))
                    if ax1_pol_idx[0].size > 0:
                        ax1.scatter(x[ax1_pol_idx], np.abs(ax1_y[ax1_pol_idx]), label=self.pol_labels[pid], edgecolors=pol_edgecolors[pid],
                                    alpha=pol_alphas[pid], color=pol_colors[pid], marker=pol_markers[pid])

                # Vertical line and annotation at major cycle end
                ax0.axvline(x0, linewidth=1, linestyle='dotted', color='k')
                ax1.axvline(x0, linewidth=1, linestyle='dotted', color='k')

                ax0.annotate(f'iter{iter}', xy=(x0, ax0.get_ylim()[0]), xycoords='data',
                             xytext=(-10, 6), textcoords='offset points', size=8, rotation=90)

        if use_ylog:
            ax0.set_yscale('log')
            ax1.set_yscale('log')

        handles, labels = ax0.get_legend_handles_labels()
        idx_list = [labels.index(x) for x in sorted(set(labels))]

        # PIPE-1401: only add the legend when num_Stokes > 1.
        if len(idx_list) > 1:
            ax1.legend([handles[idx] for idx in idx_list], [
                       '$'+labels[idx]+'$' for idx in idx_list], loc='center right')

        fig.tight_layout()
        fig.savefig(self.figfile)
        plt.close(fig)

        return self._get_plot_object()

    def _get_figfile(self):
        return os.path.join(self.reportdir,
                            self.figname+'.png')

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis=self.xlabel,
                           y_axis='/'.join(self.ylabel))


class VlassCubeSummary:
    def __init__(self, context, result):
        self.context = context
        self.result = result  # a MakeImagesResult object

    def plot(self):
        """Generate plots for the vlass cube summary."""
        plot_wrappers = []

        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        figfile = os.path.join(stage_dir, 'vlass_cube_summary.png')
        try:

            plot_beams_vlasscube(self.result.metadata['vlass_cube_metadata'], figfile)
            plot = logger.Plot(figfile,
                               x_axis='Spw Group',
                               y_axis='Beam/Flagpct',
                               parameters={})

            plot_wrappers.append(plot)

        except Exception as ex:
            LOG.warning('Could not create plot %s', figfile)
            LOG.warning(ex)

        return plot_wrappers
