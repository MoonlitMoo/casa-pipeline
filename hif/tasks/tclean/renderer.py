"""
Created on 7 Jan 2015

@author: sjw
"""
import collections
import fnmatch
import itertools
import os
import string
import traceback
from random import randint

import numpy as np

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.rendererutils as rendererutils
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tasks, casa_tools
from pipeline.infrastructure.utils.weblog import plots_to_html

from . import display

LOG = logging.get_logger(__name__)


ImageRow = collections.namedtuple('ImageInfo', (
    'vis field fieldname intent spw spwnames pol stokes_label frequency_label frequency beam beam_pa sensitivity '
    'cleaning_threshold_label cleaning_threshold initial_nsigma_mad_label initial_nsigma_mad '
    'final_nsigma_mad_label final_nsigma_mad residual_ratio non_pbcor_label non_pbcor '
    'pbcor score fractional_bw_label fractional_bw aggregate_bw_label aggregate_bw aggregate_bw_num '
    'nsigma_label nsigma vis_amp_ratio_label vis_amp_ratio  '
    'image_file datatype datatype_info nchan plot qa_url iterdone stopcode stopreason '
    'chk_pos_offset chk_frac_beam_offset chk_fitflux chk_fitpeak_fitflux_ratio img_snr '
    'chk_gfluxscale chk_gfluxscale_snr chk_fitflux_gfluxscale_ratio cube_all_cont tclean_command result '
    'model_pos_flux model_neg_flux model_flux_inner_deg nmajordone_total nmajordone_per_iter majorcycle_stat_plot '
    'tab_dict tab_url outmaskratio outmaskratio_label pol_session pol_ratio pol_angle '
    'poli_abspath poli_thumbnail pola_abspath pola_thumbnail'))
PolImagePaths = collections.namedtuple('PolImageInfo', ('poli_abspath poli_thumbnail pola_abspath pola_thumbnail'))


class T2_4MDetailsTcleanRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='tclean.mako',
                 description='Produce a cleaned image',
                 always_rerender=False):
        super(T2_4MDetailsTcleanRenderer, self).__init__(uri=uri,
                description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        # There is only ever one CleanListResult in the ResultsList as it
        # operates over multiple measurement sets, so we can set the result to
        # the first item in the list
        if not results[0]:
            return

        makeimages_result = results[0]
        clean_results = makeimages_result.results

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        qaTool = casa_tools.quanta

        # Get results info
        image_rows = []

        # Holds a mapping of image name to image stats. This information is used to scale the MOM8 images.
        image_stats = {}

        stokes_indices = {'I': 0, 'Q': 1, 'U': 2, 'V': 3}

        have_polcal_fit = False

        for r in clean_results:
            if r.empty() or not r.iterations:
                continue

            extension = '.tt0' if r.multiterm else ''
            maxiter = max(r.iterations.keys())
            if 'image' not in r.iterations[maxiter]:
                continue   # PIPE-1790: skip the rest if an image was not produced due to some error

            vis = ','.join(utils.remove_trailing_string(os.path.basename(v), '.ms') for v in r.vis)
            datatype = r.datatype
            datatype_info = r.datatype_info

            field = None
            fieldname = None
            intent = None

            image_path = r.iterations[maxiter]['image'].replace('.image', '.image%s' % extension)

            LOG.info('Getting properties of %s for the weblog' % image_path)
            with casa_tools.ImageReader(image_path) as image:
                image_name = str(image.name(strippath=True))
                info = image.miscinfo()
                coordsys = image.coordsys()
                brightness_unit = image.brightnessunit()
                summary = image.summary()
                beam = image.restoringbeam()

                # While the image tool is open, read and cache the image
                # stats for use in the plot generation classes.
                stats = image.statistics(robust=False)
                stokes_labels = coordsys.stokes()
                stokes_parameters = [stokes_labels[idx] for idx in range(image.shape()[2])]
                coordsys.done()

            for pol in stokes_parameters:
                if stokes_parameters != ['I']:
                    # Get the right stokes plane's beam and statistics
                    LOG.info('Getting properties of %s for the weblog' % image_path)
                    with casa_tools.ImagepolReader(image_path) as imagepol:
                        image = imagepol.stokes(pol)
                        #image_name = str(image.name(strippath=True))
                        info = image.miscinfo()
                        brightness_unit = image.brightnessunit()
                        summary = image.summary()
                        beam = image.restoringbeam()

                        # While the image tool is open, read and cache the image
                        # stats for use in the plot generation classes.
                        stats = image.statistics(robust=False)
                        image.close()

                # cache image statistics while we have them in scope.
                image_rms = stats.get('rms')[0]
                image_max = stats.get('max')[0]
                image_min = stats.get('min')[0]
                image_stats[image_path] = display.ImageStats(rms=image_rms, max=image_max)

                spw = info.get('virtspw', None)
                if spw is not None:
                    nspwnam = info.get('nspwnam', None)
                    spwnames = ','.join([info.get('spwnam%02d' % (i + 1)) for i in range(nspwnam)])
                else:
                    spwnames = None
                if 'field' in info:
                    field = '%s (%s)' % (info['field'], r.intent)
                    fieldname = info['field']
                    intent = r.intent

                #
                # beam calculation
                #
                if 'beams' in beam:
                    # 'beams' dict has results for each channel and
                    # each pol product. For now, just use the first beam.
                    beam = beam['beams']['*0']['*0']
                    LOG.warning('%s has per-plane beam shape, displaying only first',
                                r.iterations[maxiter]['image'].replace('.image', '.image%s' % extension))

                #
                # beam value
                #
                try:
                    beam_major = qaTool.convert(beam['major'], 'arcsec')
                    beam_minor = qaTool.convert(beam['minor'], 'arcsec')
                    row_beam = '%#.3g x %#.3g %s' % (beam_major['value'], beam_minor['value'], beam_major['unit'])
                except:
                    row_beam = '-'

                #
                # beam position angle
                #
                try:
                    beam_pa = qaTool.convert(beam['positionangle'], 'deg')
                    row_beam_pa = casa_tools.quanta.tos(beam_pa, 1)
                except:
                    row_beam_pa = '-'

                nchan = summary['shape'][3]
                width = qaTool.quantity(summary['incr'][3], summary['axisunits'][3])
                width = qaTool.convert(width, 'MHz')
                width = qaTool.tos(width, 4)

                # eff_ch_bw_MHz = qaTool.convert(r.eff_ch_bw, 'MHz')['value']
                # eff_ch_bw_text = '%.5g MHz (TOPO)' % (eff_ch_bw_MHz)
                # effective_channel_bandwidth = eff_ch_bw_text

                #
                # stokes heading
                #
                stokes_label = 'stokes'

                #
                # centre frequency heading
                #
                if nchan > 1:
                    row_frequency_label = 'centre / rest frequency of cube'
                elif nchan == 1:
                    row_frequency_label = 'centre frequency of image'
                else:
                    row_frequency_label = 'centre frequency'

                #
                # centre and optionally rest frequency value
                #
                try:
                    frequency_axis = list(summary['axisnames']).index('Frequency')
                    center_frequency = summary['refval'][frequency_axis] + \
                        (summary['shape'][frequency_axis] / 2.0 - 0.5 - summary['refpix'][frequency_axis]) \
                        * summary['incr'][frequency_axis]
                    centre_ghz = qaTool.convert('%s %s' % (center_frequency, summary['axisunits'][frequency_axis]), 'GHz')
                    if nchan > 1:
                        job = casa_tasks.imhead(image_path, mode='get', hdkey='restfreq')
                        restfreq = job.execute()
                        rest_ghz = qaTool.convert(restfreq, 'GHz')
                        row_frequency = '%s / %s (LSRK)' % (casa_tools.quanta.tos(centre_ghz, 4),
                                                            casa_tools.quanta.tos(rest_ghz, 4))
                    else:
                        row_frequency = '%s (LSRK)' % casa_tools.quanta.tos(centre_ghz, 4)
                except:
                    row_frequency = '-'

                #
                # residual peak / scaled MAD
                #
                if stokes_parameters == ['I']:
                    with casa_tools.ImageReader(r.iterations[maxiter]['residual'] + extension) as residual:
                        residual_stats = residual.statistics(robust=True)
                else:
                    with casa_tools.ImagepolReader(r.iterations[maxiter]['residual'] + extension) as residualpol:
                        residual = residualpol.stokes(pol)
                        residual_stats = residual.statistics(robust=True)
                        residualpol.close()

                residual_robust_rms = residual_stats.get('medabsdevmed')[0] * 1.4826  # see CAS-9631
                if abs(residual_stats['min'])[0] > abs(residual_stats['max'])[0]:  # see CAS-10731 & PIPE-374
                    residual_peak_value = residual_stats['min'][0]
                else:
                    residual_peak_value = residual_stats['max'][0]
                residual_snr = (residual_peak_value / residual_robust_rms)
                row_residual_ratio = '%.2f' % residual_snr
                # preserve the sign of the largest magnitude value for printout
                LOG.info('{field} clean value of maximum absolute residual / scaled MAD'
                         ' = {peak:.12f} / {rms:.12f} = {ratio:.2f} '.format(field=field,
                                                                             peak=residual_peak_value,
                                                                             rms=residual_robust_rms,
                                                                             ratio=residual_snr))

                #
                # theoretical sensitivity
                #
                if r.sensitivity is not None and r.sensitivity > 0.0:
                    sp_str, sp_scale = utils.get_si_prefix(r.sensitivity, lztol=1)
                    row_sensitivity = '{:.2g} {}'.format(r.sensitivity/sp_scale, sp_str+brightness_unit)
                else:
                    row_sensitivity = '-'

                #
                # Model image statistics for VLASS, PIPE-991
                #
                if 'VLASS-SE-CONT' in r.imaging_mode:
                    model_image = r.iterations[maxiter]['model'] + extension
                    with casa_tools.ImageReader(model_image) as image:
                        # In some cases there might not be any negative (or positive) pixels
                        try:
                            pos_flux = image.statistics(mask='"%s" > %f'%(model_image, 0.0), robust=False)['sum'][0]
                        except IndexError:
                            pos_flux = 0.0
                        row_model_pos_flux = '{:.2g} {}'.format(pos_flux, image.brightnessunit())
                        try:
                            neg_flux = image.statistics(mask='"%s" < %f'%(model_image, 0.0), robust=False)['sum'][0]
                        except IndexError:
                            neg_flux = 0.0
                        row_model_neg_flux = '{:.2g} {}'.format(neg_flux, image.brightnessunit())
                        # Create region for inner degree
                        # TODO: refactor because this code is partially a duplicate of vlassmasking.py
                        image_csys = image.coordsys()

                        xpixel = image_csys.torecord()['direction0']['crpix'][0]
                        ypixel = image_csys.torecord()['direction0']['crpix'][1]
                        xdelta = image_csys.torecord()['direction0']['cdelt'][0]  # in radians
                        ydelta = image_csys.torecord()['direction0']['cdelt'][1]  # in radians
                        onedeg = 1.0 * np.pi / 180.0  # conversion
                        widthdeg = 1.0  # degrees
                        boxhalfxwidth = np.abs((onedeg * widthdeg / 2.0) / xdelta)
                        boxhalfywidth = np.abs((onedeg * widthdeg / 2.0) / ydelta)

                        blcx = xpixel - boxhalfxwidth
                        blcy = ypixel - boxhalfywidth
                        if blcx < 0:
                            blcx = 0
                        if blcy < 0:
                            blcy = 0
                        blc = [blcx, blcy]

                        trcx = xpixel + boxhalfxwidth
                        trcy = ypixel + boxhalfywidth
                        if trcx > image.getchunk().shape[0]:
                            trcx = image.getchunk().shape[0]
                        if trcy > image.getchunk().shape[1]:
                            trcy = image.getchunk().shape[1]
                        trc = [trcx, trcy]

                        myrg = casa_tools.regionmanager
                        r1 = myrg.box(blc=blc, trc=trc)

                        y = image.getregion(r1)
                        row_model_flux_inner_deg = '{:.2g} {}'.format(y.sum(), image.brightnessunit())
                        image_csys.done()
                else:
                    row_model_pos_flux = None
                    row_model_neg_flux = None
                    row_model_flux_inner_deg = None

                row_nmajordone_per_iter, row_nmajordone_total, majorcycle_stat_plot, tab_dict = get_cycle_stats(
                    context, makeimages_result, r)

                #
                # Amount of flux inside and outside QL for VLASS-SE-CONT, PIPE-1081
                #
                if 'VLASS-SE-CONT' in r.imaging_mode and r.outmaskratio:
                    row_outmaskratio_label = 'flux fraction outside clean mask'
                    row_outmaskratio = '%#.3g' % r.outmaskratio
                else:
                    row_outmaskratio_label = None
                    row_outmaskratio = None

                #
                # clean iterations, for VLASS
                #
                if 'VLASS' in r.imaging_mode:
                    row_iterdone = r.tclean_iterdone
                    row_stopcode = r.tclean_stopcode
                    row_stopreason = r.tclean_stopreason
                else:
                    row_iterdone = None
                    row_stopcode = None
                    row_stopreason = None

                #
                # cleaning threshold cell
                #

                cleaning_threshold_label = 'cleaning threshold'

                if 'VLASS' in r.imaging_mode:
                    if r.threshold:
                        threshold_quantity = utils.get_casa_quantity(r.threshold)
                        row_cleaning_threshold = '%.2g %s' % (threshold_quantity['value'], threshold_quantity['unit'])
                    else:
                        row_cleaning_threshold = '-'
                elif 'VLA' in r.imaging_mode:
                    cleaning_threshold_label = None
                    row_cleaning_threshold = '-'
                else:
                    if r.threshold:
                        threshold_quantity = qaTool.convert(r.threshold, 'Jy')
                        sp_str, sp_scale = utils.get_si_prefix(threshold_quantity['value'], lztol=1)
                        row_cleaning_threshold = '{:.2g} {}'.format(
                            threshold_quantity['value']/sp_scale, sp_str+brightness_unit)
                        if r.dirty_dynamic_range:
                            row_cleaning_threshold += '<br>Dirty DR: %.2g' % r.dirty_dynamic_range
                            row_cleaning_threshold += '<br>DR correction: %.2g' % r.DR_correction_factor
                        else:
                            row_cleaning_threshold += '<br>No DR information'
                    else:
                        row_cleaning_threshold = '-'

                #
                # nsigma * initial and final scaled MAD for residual image, See PIPE-488
                #
                nsigma_final = r.iterations[maxiter]['imaging_params']['nsigma']

                # dirty image statistics (iter 0)
                with casa_tools.ImageReader(r.iterations[0]['residual'] + extension) as residual:
                    initial_residual_stats = residual.statistics(robust=True)

                initial_nsigma_mad = nsigma_final * initial_residual_stats.get('medabsdevmed')[0] * 1.4826
                final_nsigma_mad = nsigma_final * residual_stats.get('medabsdevmed')[0] * 1.4826

                if (nsigma_final != 0.0):
                    row_initial_nsigma_mad = '%#.3g %s' % (initial_nsigma_mad, brightness_unit)
                    row_final_nsigma_mad = '%#.3g %s' % (final_nsigma_mad, brightness_unit)
                else:
                    row_initial_nsigma_mad = '-'
                    row_final_nsigma_mad = '-'

                # store values in log file
                LOG.info('n-sigma * initial scaled MAD of residual: %s %s' % (("%.12f" % initial_nsigma_mad, brightness_unit)
                                                                               if row_initial_nsigma_mad != '-'
                                                                               else (row_initial_nsigma_mad,"")))
                LOG.info('n-sigma * final scaled MAD of residual: %s %s' % (("%.12f" % final_nsigma_mad, brightness_unit)
                                                                               if row_final_nsigma_mad != '-'
                                                                               else (row_final_nsigma_mad,"")))

                #
                # heading for non-pbcor RMS cell
                #
                if nchan is None:
                    non_pbcor_label = 'No RMS information'
                elif nchan == 1:
                    non_pbcor_label = 'non-pbcor image RMS'
                else:
                    non_pbcor_label = 'non-pbcor image RMS / RMS<sub>min</sub> / RMS<sub>max</sub>'

                #
                # value for non-pbcor RMS cell
                #
                if nchan is None or r.image_rms is None:
                    row_non_pbcor = '-'
                else:
                    if stokes_parameters != ['I']:
                        r_image_rms = r.image_rms_iquv[stokes_indices[pol]]
                        r_image_rms_max = r.image_rms_iquv[stokes_indices[pol]]
                        r_image_rms_min = r.image_rms_iquv[stokes_indices[pol]]
                    else:
                        r_image_rms = r.image_rms
                        r_image_rms_max = r.image_rms_max
                        r_image_rms_min = r.image_rms_min
                    sp_str, sp_scale = utils.get_si_prefix(r_image_rms, lztol=1)
                    if nchan == 1:
                        row_non_pbcor = '{:.2g} {}'.format(r_image_rms/sp_scale, sp_str+brightness_unit)
                    else:
                        row_non_pbcor = '{:.2g} / {:.2g} / {:.2g} {}'.format(
                            r_image_rms/sp_scale, r_image_rms_min/sp_scale, r_image_rms_max/sp_scale, sp_str+brightness_unit)

                #
                # pbcor image max / min cell
                #
                if r.image_max is None or r.image_min is None:
                    row_pbcor = '-'
                else:
                    if stokes_parameters != ['I']:
                        r_image_max = r.image_max_iquv[stokes_indices[pol]]
                        r_image_min = r.image_min_iquv[stokes_indices[pol]]
                    else:
                        r_image_max = r.image_max
                        r_image_min = r.image_min
                    sp_str, sp_scale = utils.get_si_prefix(r_image_max, lztol=0)
                    row_pbcor = '{:.3g} / {:.3g} {}'.format(r_image_max/sp_scale,
                                                            r_image_min/sp_scale, sp_str+brightness_unit)

                #
                # fractional bandwidth calculation
                #
                try:
                    frequency1 = summary['refval'][frequency_axis] + (-0.5 - summary['refpix'][frequency_axis]) * summary['incr'][frequency_axis]
                    frequency2 = summary['refval'][frequency_axis] + (summary['shape'][frequency_axis] - 0.5 - summary['refpix'][frequency_axis]) * summary['incr'][frequency_axis]
                    # full_bw_GHz = qaTool.convert(abs(frequency2 - frequency1), 'GHz')['value']
                    fractional_bw = (frequency2 - frequency1) / (0.5 * (frequency1 + frequency2))
                    fractional_bandwidth = '%.2g%%' % (fractional_bw * 100.)
                except:
                    fractional_bandwidth = 'N/A'

                #
                # fractional bandwidth heading and value
                #
                nterms = r.multiterm if r.multiterm else 1
                if nchan is None:
                    row_fractional_bw_label = 'No channel / width information'
                    row_fractional_bw = '-'
                elif nchan > 1:
                    row_fractional_bw_label = 'channels'
                    if r.orig_specmode == 'repBW':
                        row_fractional_bw = '%d x %s (repBW, LSRK)' % (nchan, width)
                    else:
                        row_fractional_bw = '%d x %s (LSRK)' % (nchan, width)
                else:
                    row_fractional_bw_label = 'fractional bandwidth / nterms'
                    row_fractional_bw = '%s / %s' % (fractional_bandwidth, nterms)

                #
                # aggregate bandwidth heading
                #
                if nchan == 1:
                    row_bandwidth_label = 'aggregate bandwidth'
                else:
                    row_bandwidth_label = None

                #
                # aggregate bandwidth value
                #
                aggregate_bw_GHz = qaTool.convert(r.aggregate_bw, 'GHz')['value']
                row_aggregate_bw = '%.3g GHz (LSRK)' % aggregate_bw_GHz
                row_aggregate_bw_num = '%.4g' % aggregate_bw_GHz

                #
                # VLA statistics (PIPE-764)
                #
                initial_nsigma_mad_label = None
                final_nsigma_mad_label = None

                if 'VLA' in r.imaging_mode:   # VLA and VLASS
                    initial_nsigma_mad_label = 'n-sigma * initial scaled MAD of residual'
                    final_nsigma_mad_label = 'n-sigma * final scaled MAD of residual'

                nsigma_label = None
                row_nsigma = None
                vis_amp_ratio_label = None
                row_vis_amp_ratio = None

                if 'VLA' == r.imaging_mode:  # VLA only
                    nsigma_label = 'nsigma'
                    row_nsigma = nsigma_final
                    vis_amp_ratio_label = 'vis. amp. ratio'
                    row_vis_amp_ratio = r.bl_ratio

                #
                #  score value
                #
                if r.qa.representative is not None:
                    badge_class = rendererutils.get_badge_class(r.qa.representative)
                    if r.qa.representative.score is None:
                        row_score = '<span class="badge {:s}">N/A</span>'.format(badge_class)
                    else:
                        row_score = '<span class="badge %s">%0.2f</span>' % (badge_class, r.qa.representative.score)
                else:
                    row_score = '-'

                #
                # check source fit parameters
                #
                if r.check_source_fit is not None:
                    try:
                        chk_pos_offset = '%.2f +/- %.2f' % (r.check_source_fit['offset'], r.check_source_fit['offset_err'])
                    except:
                        chk_pos_offset = 'N/A'
                    try:
                        chk_frac_beam_offset = '%.2f +/- %.3f' % (r.check_source_fit['beams'], r.check_source_fit['beams_err'])
                    except:
                        chk_frac_beam_offset = 'N/A'
                    try:
                        chk_fitflux = '%d +/- %d' % (int(utils.round_half_up(r.check_source_fit['fitflux'] * 1000.)), int(utils.round_half_up(r.check_source_fit['fitflux_err'] * 1000.)))
                    except:
                        chk_fitflux = 'N/A'

                    if r.check_source_fit['fitflux'] != 0.0:
                        try:
                            chk_fitpeak_fitflux_ratio = '%.2f' % (r.check_source_fit['fitpeak'] / r.check_source_fit['fitflux'])
                        except:
                            chk_fitpeak_fitflux_ratio = 'N/A'
                    else:
                        chk_fitpeak_fitflux_ratio = 'N/A'

                    if r.check_source_fit['gfluxscale'] is not None and r.check_source_fit['gfluxscale_err'] is not None:
                        try:
                            chk_gfluxscale = '%.2f +/- %.2f' % (r.check_source_fit['gfluxscale'], r.check_source_fit['gfluxscale_err'])
                        except:
                            chk_gfluxscale = 'N/A'

                        if r.check_source_fit['gfluxscale_err'] != 0.0:
                            try:
                                chk_gfluxscale_snr = '%.2f' % (r.check_source_fit['gfluxscale'] / r.check_source_fit['gfluxscale_err'])
                            except:
                                chk_gfluxscale_snr = 'N/A'
                        else:
                            chk_gfluxscale_snr = 'N/A'

                        if r.check_source_fit['gfluxscale'] != 0.0:
                            try:
                                chk_fitflux_gfluxscale_ratio = '%.2f' % (r.check_source_fit['fitflux'] * 1000. / r.check_source_fit['gfluxscale'])
                            except:
                                chk_fitflux_gfluxscale_ratio = 'N/A'
                        else:
                            chk_fitflux_gfluxscale_ratio = 'N/A'

                    else:
                        chk_gfluxscale = 'N/A'
                        chk_gfluxscale_snr = 'N/A'
                        chk_fitflux_gfluxscale_ratio = 'N/A'
                else:
                    chk_pos_offset = 'N/A'
                    chk_frac_beam_offset = 'N/A'
                    chk_fitflux = 'N/A'
                    chk_fitpeak_fitflux_ratio = 'N/A'
                    chk_gfluxscale = 'N/A'
                    chk_gfluxscale_snr = 'N/A'
                    chk_fitflux_gfluxscale_ratio = 'N/A'

                #
                # Polarization calibrator fit parameters
                #
                if r.polcal_fit is not None:
                    have_polcal_fit = True
                    pol_session = r.polcal_fit['session']
                    if r.polcal_fit['pol_ratio'] != 'N/A':
                        pol_ratio = f"{qaTool.getvalue(r.polcal_fit['pol_ratio'])[0]:6.2f} +/- {qaTool.getvalue(r.polcal_fit['err_pol_ratio'])[0]:6.2f}%"
                    else:
                        pol_ratio = 'N/A'
                    if r.polcal_fit['pol_angle'] != 'N/A':
                        pol_angle = f"{qaTool.getvalue(r.polcal_fit['pol_angle'])[0]:7.2f} +/- {qaTool.getvalue(r.polcal_fit['err_pol_angle'])[0]:7.2f} deg"
                    else:
                        pol_angle = 'N/A'
                else:
                    pol_session = 'N/A'
                    pol_ratio = 'N/A'
                    pol_angle = 'N/A'

                if r.image_max is not None and r.image_rms is not None:
                    if stokes_parameters != ['I']:
                        r_image_max = r.image_max_iquv[stokes_indices[pol]]
                        r_image_rms = r.image_rms_iquv[stokes_indices[pol]]
                    else:
                        r_image_max = r.image_max
                        r_image_rms = r.image_rms
                    try:
                        img_snr = '%.2f' % (r_image_max / r_image_rms)
                    except:
                        img_snr = 'N/A'
                else:
                    img_snr = 'N/A'

                cube_all_cont = r.cube_all_cont

                tclean_command = r.tclean_command

                # create our table row for this image.
                # Plot is set to None as we have a circular dependency: the row
                # needs the plot, but the plot generator needs the image_stats
                # cache. We will later set plot to the correct value.
                row = ImageRow(
                    vis=vis,
                    datatype=datatype,
                    datatype_info=datatype_info,
                    field=field,
                    fieldname=fieldname,
                    intent=intent,
                    spw=spw,
                    spwnames=spwnames,
                    pol=pol,
                    stokes_label=stokes_label,
                    frequency_label=row_frequency_label,
                    frequency=row_frequency,
                    beam=row_beam,
                    beam_pa=row_beam_pa,
                    sensitivity=row_sensitivity,
                    cleaning_threshold_label=cleaning_threshold_label,
                    cleaning_threshold=row_cleaning_threshold,
                    initial_nsigma_mad_label=initial_nsigma_mad_label,
                    initial_nsigma_mad=row_initial_nsigma_mad,
                    final_nsigma_mad_label=final_nsigma_mad_label,
                    final_nsigma_mad=row_final_nsigma_mad,
                    model_pos_flux=row_model_pos_flux,
                    model_neg_flux=row_model_neg_flux,
                    model_flux_inner_deg=row_model_flux_inner_deg,
                    nmajordone_total=row_nmajordone_total,
                    nmajordone_per_iter=row_nmajordone_per_iter,
                    majorcycle_stat_plot=majorcycle_stat_plot,
                    tab_dict=tab_dict,
                    tab_url=None,
                    residual_ratio=row_residual_ratio,
                    non_pbcor_label=non_pbcor_label,
                    non_pbcor=row_non_pbcor,
                    pbcor=row_pbcor,
                    score=row_score,
                    fractional_bw_label=row_fractional_bw_label,
                    fractional_bw=row_fractional_bw,
                    aggregate_bw_label=row_bandwidth_label,
                    aggregate_bw=row_aggregate_bw,
                    aggregate_bw_num=row_aggregate_bw_num,
                    nsigma_label=nsigma_label,
                    nsigma=row_nsigma,
                    vis_amp_ratio_label=vis_amp_ratio_label,
                    vis_amp_ratio=row_vis_amp_ratio,
                    image_file=image_name.replace('.pbcor', ''),
                    nchan=nchan,
                    plot=None,
                    qa_url=None,
                    outmaskratio=row_outmaskratio,
                    outmaskratio_label=row_outmaskratio_label,
                    iterdone=row_iterdone,
                    stopcode=row_stopcode,
                    stopreason=row_stopreason,
                    chk_pos_offset=chk_pos_offset,
                    chk_frac_beam_offset=chk_frac_beam_offset,
                    chk_fitflux=chk_fitflux,
                    chk_fitpeak_fitflux_ratio=chk_fitpeak_fitflux_ratio,
                    img_snr=img_snr,
                    chk_gfluxscale=chk_gfluxscale,
                    chk_gfluxscale_snr=chk_gfluxscale_snr,
                    chk_fitflux_gfluxscale_ratio=chk_fitflux_gfluxscale_ratio,
                    cube_all_cont=cube_all_cont,
                    tclean_command=tclean_command,
                    pol_session=pol_session,
                    pol_ratio=pol_ratio,
                    pol_angle=pol_angle,
                    poli_abspath=None,
                    poli_thumbnail=None,
                    pola_abspath=None,
                    pola_thumbnail=None,
                    result=r
                )
                image_rows.append(row)

        # PIPE-2191/PIPE-2022: create and attach a warning-level logging handler with a content filter only capturing
        # png-missing warning messages.
        extra_logrecords_handler = logging.CapturingHandler(logging.WARNING)
        extra_logrecords_handler.addFilter(missing_png_filter)
        logging.add_handler(extra_logrecords_handler)

        try:
            plotter = display.CleanSummary(context, makeimages_result, image_stats)
            plots = plotter.plot()

            plots_dict = make_plot_dict(plots)

            # construct the renderers so we know what the back/forward links will be
            temp_urls = (None, None, None)
            qa_renderers = [TCleanPlotsRenderer(context, results, row.result, plots_dict, row.image_file.split(
                '.')[0], row.field, str(row.spw), row.pol, row.datatype, temp_urls, row.cube_all_cont) for row in image_rows]
            qa_links = triadwise([renderer.path for renderer in qa_renderers])

            # PIPE-991: render tclean major cycle table, but only if tab_dict is specified (currently VLASS-SE-CONT)
            tab_renderer = [TCleanTablesRenderer(context, results, row.result,
                                                 row.tab_dict, row.image_file.split('.')[0], row.field, str(row.spw),
                                                 row.pol, temp_urls) if row.tab_dict else None for row in image_rows]
            tab_links = triadwise([renderer.path if renderer else None for renderer in tab_renderer])

            final_rows = []
            for row, renderer, qa_urls, tab_url in zip(image_rows, qa_renderers, qa_links, tab_links):
                prefix = row.image_file.split('.')[0]
                try:
                    final_iter = sorted(plots_dict[prefix][row.datatype][row.field][str(row.spw)][row.pol].keys())[-1]
                    # cube and repBW mode use mom8
                    plot = get_plot(plots_dict, prefix, row.datatype, row.field, str(row.spw), row.pol, final_iter, 'image', 'mom8')
                    if plot is None:
                        # mfs and cont mode use mean
                        plot = get_plot(plots_dict, prefix, row.datatype, row.field, str(row.spw), row.pol, final_iter, 'image', 'mean')

                    renderer = TCleanPlotsRenderer(context, results, row.result,
                                                   plots_dict, prefix, row.field, str(row.spw), row.pol,
                                                   row.datatype, qa_urls, row.cube_all_cont)
                    with renderer.get_file() as fileobj:
                        fileobj.write(renderer.render())

                    values = row._asdict()
                    values['plot'] = plot
                    values['qa_url'] = renderer.path

                    # PIPE-991: render tclean major cycle table, but only if tab_dict exists (currently VLASS-SE-CONT)
                    if any(tab_url):
                        tab_renderer = TCleanTablesRenderer(context, results, row.result,
                                                            row.tab_dict, prefix, row.field, str(row.spw), row.pol,
                                                            tab_url)
                        with tab_renderer.get_file() as fileobj:
                            fileobj.write(tab_renderer.render())
                        values['tab_url'] = tab_renderer.path

                    if stokes_parameters != ['I']:
                        # Save POLI/POLA paths which is known only after plot() has been called
                        values['poli_abspath'] = get_plot(
                            plots_dict, prefix, row.datatype, row.field, str(row.spw),
                            'Ptotal', final_iter, 'image', 'mean').abspath
                        values['poli_thumbnail'] = get_plot(
                            plots_dict, prefix, row.datatype, row.field, str(row.spw),
                            'Ptotal', final_iter, 'image', 'mean').thumbnail
                        values['pola_abspath'] = get_plot(
                            plots_dict, prefix, row.datatype, row.field, str(row.spw),
                            'Pangle', final_iter, 'image', 'mean').abspath
                        values['pola_thumbnail'] = get_plot(
                            plots_dict, prefix, row.datatype, row.field, str(row.spw),
                            'Pangle', final_iter, 'image', 'mean').thumbnail

                    new_row = ImageRow(**values)
                    final_rows.append(new_row)
                except IOError as e:
                    LOG.error(e)
                except Exception as e:
                    # Probably some detail page rendering exception.
                    LOG.error(e)
                    final_rows.append(row)

            # PIPE-1595: sort targets by field/spw/pol for VLA, so multiple bands of the same objects will
            # stay in the same weblog table row. Note that this additional VLA-only sorting might introduce
            # a difference between the target sequences of hif_makeimages and hif_makeimlist (see PIPE-1302).
            if final_rows and 'VLA' in final_rows[0].result.imaging_mode:
                final_rows.sort(key=lambda row: (row.vis, row.datatype, row.field, utils.natural_sort_key(row.spw), row.pol))

            chk_fit_rows = []
            for row in final_rows:
                if row.frequency is not None:
                    chk_fit_rows.append((row.vis, row.fieldname, row.spw, row.aggregate_bw_num, row.chk_pos_offset, row.chk_frac_beam_offset, row.chk_fitflux,
                                        row.img_snr, row.chk_fitpeak_fitflux_ratio, row.chk_gfluxscale, row.chk_gfluxscale_snr, row.chk_fitflux_gfluxscale_ratio))
            chk_fit_rows = utils.merge_td_columns(chk_fit_rows, num_to_merge=2)

            pol_fit_rows = []
            pol_fit_plots = []
            for row in final_rows:
                if row.pol == 'I':
                    # Save only once for weblog because the fit is the same for all Stokes parameters
                    pol_fit_rows.append((row.pol_session, row.vis, row.fieldname, row.spw, row.pol_ratio, row.pol_angle))
                    pol_fit_plots.append(PolImagePaths(poli_abspath=row.poli_abspath,
                                                       poli_thumbnail=row.poli_thumbnail,
                                                       pola_abspath=row.pola_abspath,
                                                       pola_thumbnail=row.pola_thumbnail))
            pol_fit_rows = utils.merge_td_columns(pol_fit_rows, num_to_merge=4)

            # PIPE-1723: display a message in the weblog depending on the observatory
            imaging_mode = clean_results[0].imaging_mode if len(clean_results) > 0 else None

        finally:
            # PIPE-2191/PIPE-2022: remove the local logging handler and attach the LogRecord list to extra_logrecords.
            logging.remove_handler(extra_logrecords_handler)
            extra_logrecords = extra_logrecords_handler.buffer

        ctx.update({
            'imaging_mode': imaging_mode,
            'plots': plots,
            'plots_dict': plots_dict,
            'image_info': final_rows,
            'dirname': weblog_dir,
            'have_polcal_fit': have_polcal_fit,
            'chk_fit_info': chk_fit_rows,
            'pol_fit_info': pol_fit_rows,
            'pol_fit_plots': pol_fit_plots,
            'extra_logrecords': extra_logrecords
        })


class TCleanPlotsRenderer(basetemplates.CommonRenderer):
    def __init__(self, context, makeimages_results, result, plots_dict, prefix, field, spw, pol, datatype, urls, cube_all_cont):
        super(TCleanPlotsRenderer, self).__init__('tcleanplots.mako', context, makeimages_results)

        # Set HTML page name
        # VLA needs a slightly different name for some cases
        # For that we need to check imaging_mode and specmode but we have to
        # protect against iteration errors for empty results.
        if not result.empty():
            if 'VLA' in result.imaging_mode and 'VLASS' not in result.imaging_mode and result.specmode == 'cont':
                # ms = context.observing_run.get_ms(result[0].results[0].vis[0])
                # band = ms.get_vla_spw2band()
                # band_spws = {}
                # for k, v in band.items():
                #     band_spws.setdefault(v, []).append(k)
                # for k, v in band_spws.items():
                #     for spw in spw.split(','):
                #         if int(spw) in v:
                #             band = k
                #             break
                # TODO: Not sure if a random number will work in all cases.
                #       While working on PIPE-129 it happened that this code
                #       was run 4 times for 2 targets. Better make sure the
                #       name is well defined (see new setup for per EB images below).
                outfile = '%s-field%s-pol%s-datatype%s-cleanplots-%d.html' % (prefix, field, pol, datatype, randint(1, 1e12))
            else:
                # The name needs to be unique also for the per EB imaging. Thus prepend the image name
                # which contains the OUS or EB ID.
                outfile = '%s-field%s-spw%s-pol%s-datatype%s-cleanplots.html' % (prefix, field, spw, pol, datatype)
        # TODO: Check if this is useful since the result is empty.
        else:
            outfile = '%s-field%s-spw%s-pol%s-datatype%s-cleanplots.html' % (prefix, field, spw, pol, datatype)

        # HTML encoded filenames, so can't have plus sign
        valid_chars = "_.-%s%s" % (string.ascii_letters, string.digits)
        self.path = os.path.join(self.dirname, filenamer.sanitize(outfile, valid_chars))

        if result.specmode in ('mfs', 'cont'):
            colorders = [[('pbcorimage', None), ('residual', None), ('cleanmask', None)]]
        else:
            colorders = [[('pbcorimage', 'mom8'), ('residual', 'mom8'), ('mom8_fc', None), ('spectra', None)],
                         [('pbcorimage', 'mom0'), ('residual', 'mom0'), ('mom0_fc', None), ('cleanmask', None)]]

        if 'VLA' in result.imaging_mode:
            # PIPE-1462: use non-pbcor images for VLA in the tclean details page.
            # Because 'mtmfs' CASA/tclean doesn't generate pbcor images for VLA and silently passes with a warning when pbcor=True,
            # pbcor images are not produced from hif.tasks.tclean (see PIPE-1201/CAS-11636)
            # Here, we set a fallback with non-pbcor images.
            for i, colorder in enumerate(colorders):
                colorders[i] = [('image', moment) if im_type == 'pbcorimage' else (im_type, moment) for im_type, moment in colorder]

        self.extra_data = {
            'plots_dict': plots_dict,
            'prefix': prefix.split('.')[0],
            'datatype': datatype,
            'field': field,
            'spw': spw,
            'pol': pol,
            'qa_previous': urls[0],
            'qa_next': urls[2],
            'base_url': os.path.join(self.dirname, 't2-4m_details.html'),
            'cube_all_cont': cube_all_cont,
            'cube_mode': result.specmode in ('cube', 'repBW'),
            'colorders': colorders
        }

    def update_mako_context(self, mako_context):
        mako_context.update(self.extra_data)


class TCleanTablesRenderer(basetemplates.CommonRenderer):
    def __init__(self, context, makeimages_results, result, table_dict, prefix, field, spw, pol, urls):
        super(TCleanTablesRenderer, self).__init__('tcleantables.mako', context, makeimages_results)

        # Set HTML page name
        outfile = '%s-field%s-spw%s-pol%s-cleantables.html' % (prefix, field, spw, pol)

        # HTML encoded filenames, so can't have plus sign
        valid_chars = "_.-%s%s" % (string.ascii_letters, string.digits)
        self.path = os.path.join(self.dirname, filenamer.sanitize(outfile, valid_chars))

        self.extra_data = {
            'table_dict': table_dict,
            'prefix': prefix.split('.')[0],
            'field': field,
            'spw': spw,
            'pol': pol,
            'qa_previous': urls[0],
            'qa_next': urls[2],
            'base_url': os.path.join(self.dirname, 't2-4m_details.html'),
        }

    def update_mako_context(self, mako_context):
        mako_context.update(self.extra_data)


def get_plot(plots, prefix, datatype, field, spw, stokes, i, colname, moment):
    try:
        plot = plots[prefix][datatype][field][spw][stokes][i][colname][moment]
        if not os.path.exists(plot.abspath):
            # PIPE-2022: Generate a warning if the PNG file is missing. The
            # message is caught by a local logging handler for the weblog.
            LOG.warning(f'Plot {plot.abspath} is missing on disk')
        return plot
    except KeyError:
        return None


def make_plot_dict(plots):
    # Make the plots
    # Note that any change in the nested dictionary structure must be
    # reflected in the renderer class code and in the Mako templates.
    # Otherwise there may be inadvertent modifications of the dictionary
    # when it is passed to the render() method.
    prefixes = sorted({p.parameters['prefix'] for p in plots})
    datatypes = sorted({p.parameters['datatype'] for p in plots})
    fields = sorted({p.parameters['field'] for p in plots})
    spws = sorted({p.parameters['virtspw'] for p in plots})
    stokeses = sorted({p.parameters['stokes'] for p in plots})
    iterations = sorted({p.parameters['iter'] for p in plots})
    types = sorted({p.parameters['type'] for p in plots})
    moments = sorted({p.parameters['moment'] for p in plots})

    type_dim = lambda: collections.defaultdict(dict)
    iteration_dim = lambda: collections.defaultdict(type_dim)
    spw_dim = lambda: collections.defaultdict(iteration_dim)
    stokes_dim = lambda: collections.defaultdict(spw_dim)
    field_dim = lambda: collections.defaultdict(stokes_dim)
    datatype_dim = lambda: collections.defaultdict(field_dim)
    plots_dict = collections.defaultdict(datatype_dim)
    for prefix in prefixes:
        for datatype in datatypes:
            for field in fields:
                for spw in spws:
                    for stokes in stokeses:
                        for iteration in iterations:
                            for t in types:
                                for moment in moments:
                                    matching = [p for p in plots
                                                if p.parameters['prefix'] == prefix
                                                and p.parameters['datatype'] == datatype
                                                and p.parameters['field'] == field
                                                and p.parameters['virtspw'] == spw
                                                and p.parameters['stokes'] == stokes
                                                and p.parameters['iter'] == iteration
                                                and p.parameters['type'] == t
                                                and p.parameters['moment'] == moment]
                                    if matching:
                                        plots_dict[prefix][datatype][field][spw][stokes][iteration][t][moment] = matching[0]

    return plots_dict


def triadwise(iterable):
    with_nones = [None] + list(iterable) + [None]
    "s -> (s0,s1,s2), (s1,s2,s3), (s2,s3,s4), ..."
    a, b, c = itertools.tee(with_nones, 3)
    next(b, None)
    next(c, None)
    next(c, None)
    return list(zip(a, b, c))


class T2_4MDetailsTcleanVlassCubeRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """A vlass-cube-specific renderer class to handle full-stokes imaging produdcts."""

    def __init__(self, uri='vlasscube_tclean.mako',
                 description='Produce a cleaned image',
                 always_rerender=False):
        super().__init__(uri=uri,
                         description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):

        # because hif.tclean is a multi-vis task (is_multi_vis_task = True) which operates over multiple MSs,
        # we will only get one CleanListResult in the ResultsList returned by the task.
        makeimages_result = results[0]
        if not makeimages_result:
            return

        clean_results = makeimages_result.results
        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        qaTool = casa_tools.quanta

        # Get results info
        image_rows = []
        # Holds a mapping of image name to image stats. This information is used to scale the MOM8 images.
        image_stats = {}

        stokes_indices = {'I': 0, 'Q': 1, 'U': 2, 'V': 3}

        have_polcal_fit = False

        for r in clean_results:

            if r.empty() or not r.iterations:
                continue

            extension = '.tt0' if r.multiterm else ''
            maxiter = max(r.iterations.keys())
            field = fieldname = intent = None

            vis = ','.join(utils.remove_trailing_string(os.path.basename(v), '.ms') for v in r.vis)
            datatype = r.datatype
            datatype_info = r.datatype_info
            image_path = r.iterations[maxiter]['image'].replace('.image', f'.image{extension}')

            LOG.info('Getting properties of %s for the weblog' % image_path)
            with casa_tools.ImageReader(image_path) as image:
                image_name = str(image.name(strippath=True))
                info = image.miscinfo()
                coordsys = image.coordsys()
                brightness_unit = image.brightnessunit()
                summary = image.summary()
                beam = image.restoringbeam()

                # While the image tool is open, read and cache the image
                # stats for use in the plot generation classes.
                stats = image.statistics(robust=False)
                stokes_labels = coordsys.stokes()
                stokes_parameters = [stokes_labels[idx] for idx in range(image.shape()[2])]

            for pol in stokes_parameters:

                LOG.info('Getting properties of %s for the weblog' % image_path)
                with casa_tools.ImagepolReader(image_path) as imagepol:
                    image = imagepol.stokes(pol)
                    #image_name = str(image.name(strippath=True))
                    info = image.miscinfo()
                    coordsys = image.coordsys()
                    brightness_unit = image.brightnessunit()
                    summary = image.summary()
                    beam = image.restoringbeam()

                    # While the image tool is open, read and cache the image
                    # stats for use in the plot generation classes.
                    stats = image.statistics(robust=False)
                    image.close()

                # cache image statistics while we have them in scope.
                image_rms = stats.get('rms')[0]
                image_max = stats.get('max')[0]
                image_min = stats.get('min')[0]
                image_stats[image_path] = display.ImageStats(rms=image_rms, max=image_max)

                spw = info.get('virtspw', None)
                if spw is not None:
                    nspwnam = info.get('nspwnam', None)
                    spwnames = ','.join([info.get('spwnam%02d' % (i + 1)) for i in range(nspwnam)])
                else:
                    spwnames = None
                if 'field' in info:
                    field = '%s (%s)' % (info['field'], r.intent)
                    fieldname = info['field']
                    intent = r.intent

                coordsys.done()

                #
                # beam calculation
                #
                if 'beams' in beam:
                    # 'beams' dict has results for each channel and
                    # each pol product. For now, just use the first beam.
                    beam = beam['beams']['*0']['*0']
                    LOG.warning('%s has per-plane beam shape, displaying only first',
                                r.iterations[maxiter]['image'].replace('.image', '.image%s' % extension))

                #
                # beam value
                #
                try:
                    beam_major = qaTool.convert(beam['major'], 'arcsec')
                    beam_minor = qaTool.convert(beam['minor'], 'arcsec')
                    row_beam = '%#.3g x %#.3g %s' % (beam_major['value'], beam_minor['value'], beam_major['unit'])
                except:
                    row_beam = '-'

                #
                # beam position angle
                #
                try:
                    beam_pa = qaTool.convert(beam['positionangle'], 'deg')
                    row_beam_pa = casa_tools.quanta.tos(beam_pa, 1)
                except:
                    row_beam_pa = '-'

                nchan = summary['shape'][3]
                width = qaTool.quantity(summary['incr'][3], summary['axisunits'][3])
                width = qaTool.convert(width, 'MHz')
                width = qaTool.tos(width, 4)

                # eff_ch_bw_MHz = qaTool.convert(r.eff_ch_bw, 'MHz')['value']
                # eff_ch_bw_text = '%.5g MHz (TOPO)' % (eff_ch_bw_MHz)
                # effective_channel_bandwidth = eff_ch_bw_text

                #
                # centre frequency heading
                #
                if nchan > 1:
                    row_frequency_label = 'centre / rest frequency of cube'
                elif nchan == 1:
                    row_frequency_label = 'centre frequency of image'
                else:
                    row_frequency_label = 'centre frequency'

                #
                # centre and optionally rest frequency value
                #
                try:
                    frequency_axis = list(summary['axisnames']).index('Frequency')
                    center_frequency = summary['refval'][frequency_axis] + \
                        (summary['shape'][frequency_axis] / 2.0 - 0.5 - summary['refpix'][frequency_axis]) \
                        * summary['incr'][frequency_axis]
                    centre_ghz = qaTool.convert('%s %s' % (
                        center_frequency, summary['axisunits'][frequency_axis]), 'GHz')
                    if nchan > 1:
                        job = casa_tasks.imhead(image_path, mode='get', hdkey='restfreq')
                        restfreq = job.execute()
                        rest_ghz = qaTool.convert(restfreq, 'GHz')
                        row_frequency = '%s / %s (LSRK)' % (casa_tools.quanta.tos(centre_ghz, 4),
                                                            casa_tools.quanta.tos(rest_ghz, 4))
                    else:
                        row_frequency = '%s (LSRK)' % casa_tools.quanta.tos(centre_ghz, 4)
                except:
                    row_frequency = '-'

                #
                # residual peak / scaled MAD
                #
                with casa_tools.ImagepolReader(r.iterations[maxiter]['residual'] + extension) as residualpol:
                    residual = residualpol.stokes(pol)
                    residual_stats = residual.statistics(robust=True)
                    residualpol.close()

                residual_robust_rms = residual_stats.get('medabsdevmed')[0] * 1.4826  # see CAS-9631
                if abs(residual_stats['min'])[0] > abs(residual_stats['max'])[0]:  # see CAS-10731 & PIPE-374
                    residual_peak_value = residual_stats['min'][0]
                else:
                    residual_peak_value = residual_stats['max'][0]
                residual_snr = (residual_peak_value / residual_robust_rms)
                row_residual_ratio = '%.2f' % residual_snr
                # preserve the sign of the largest magnitude value for printout

                LOG.info('{field} clean value of maximum absolute residual / scaled MAD'
                         ' = {peak:.12f} / {rms:.12f} = {ratio:.2f} '.format(field=field,
                                                                             peak=residual_peak_value,
                                                                             rms=residual_robust_rms,
                                                                             ratio=residual_snr))

                #
                # theoretical sensitivity
                #
                if 'VLA' in r.imaging_mode:
                    row_sensitivity = '-'
                else:
                    sp_str, sp_scale = utils.get_si_prefix(r.sensitivity, lztol=1)
                    row_sensitivity = '{:.2g} {}'.format(r.sensitivity/sp_scale, sp_str+brightness_unit)

                #
                # Model image statistics for VLASS, PIPE-991
                #
                if 'VLASS-SE-CUBE' in r.imaging_mode:
                    model_image = r.iterations[maxiter]['model'] + extension
                    with casa_tools.ImageReader(model_image) as image:

                        image_csys = image.coordsys()
                        rgTool = casa_tools.regionmanager
                        region = rgTool.frombcs(csys=image_csys.torecord(), shape=image.shape(),
                                                stokes=pol, stokescontrol='a')
                        image_csys.done()
                        rgTool.done()

                        # In some cases there might not be any negative (or positive) pixels
                        try:
                            pos_flux = image.statistics(region=region, mask='"%s" > %f' %
                                                        (model_image, 0.0), robust=False)['sum'][0]
                        except IndexError:
                            pos_flux = 0.0
                        row_model_pos_flux = '{:.2g} {}'.format(pos_flux, image.brightnessunit())
                        try:
                            neg_flux = image.statistics(region=region, mask='"%s" < %f' %
                                                        (model_image, 0.0), robust=False)['sum'][0]
                        except IndexError:
                            neg_flux = 0.0
                        row_model_neg_flux = '{:.2g} {}'.format(neg_flux, image.brightnessunit())
                        # Create region for inner degree
                        # TODO: refactor because this code is partially a duplicate of vlassmasking.py
                        image_csys = image.coordsys()

                        xpixel = image_csys.torecord()['direction0']['crpix'][0]
                        ypixel = image_csys.torecord()['direction0']['crpix'][1]
                        xdelta = image_csys.torecord()['direction0']['cdelt'][0]  # in radians
                        ydelta = image_csys.torecord()['direction0']['cdelt'][1]  # in radians
                        onedeg = 1.0 * np.pi / 180.0  # conversion
                        widthdeg = 1.0  # degrees
                        boxhalfxwidth = np.abs((onedeg * widthdeg / 2.0) / xdelta)
                        boxhalfywidth = np.abs((onedeg * widthdeg / 2.0) / ydelta)

                        blcx = xpixel - boxhalfxwidth
                        blcy = ypixel - boxhalfywidth
                        if blcx < 0:
                            blcx = 0
                        if blcy < 0:
                            blcy = 0

                        trcx = xpixel + boxhalfxwidth
                        trcy = ypixel + boxhalfywidth
                        if trcx > image.getchunk().shape[0]:
                            trcx = image.getchunk().shape[0]
                        if trcy > image.getchunk().shape[1]:
                            trcy = image.getchunk().shape[1]

                        image_csys = image.coordsys()
                        rgTool = casa_tools.regionmanager
                        shape_inner = image.shape()
                        shape_inner[0] = boxhalfxwidth*2.0
                        shape_inner[1] = boxhalfywidth*2.0
                        region = rgTool.frombcs(csys=image_csys.torecord(), shape=image.shape(),
                                                stokes=pol, stokescontrol='a')
                        image_csys.done()
                        rgTool.done()
                        y = image.getregion(region)

                        row_model_flux_inner_deg = '{:.2g} {}'.format(y.sum(), image.brightnessunit())

                else:
                    row_model_pos_flux = None
                    row_model_neg_flux = None
                    row_model_flux_inner_deg = None

                row_nmajordone_per_iter, row_nmajordone_total, majorcycle_stat_plot, tab_dict = get_cycle_stats(
                    context, makeimages_result, r)

                #
                # Amount of flux inside and outside QL for VLASS-SE-CONT, PIPE-1081
                #
                if 'VLASS-SE-CUBE' in r.imaging_mode and r.outmaskratio:
                    row_outmaskratio_label = 'flux fraction outside clean mask'
                    row_outmaskratio = '%#.3g' % r.outmaskratio
                else:
                    row_outmaskratio_label = None
                    row_outmaskratio = None

                #
                # clean iterations, for VLASS
                #
                if 'VLASS' in r.imaging_mode:
                    row_iterdone = r.tclean_iterdone
                    row_stopcode = r.tclean_stopcode
                    row_stopreason = r.tclean_stopreason
                else:
                    row_iterdone = None
                    row_stopcode = None
                    row_stopreason = None

                #
                # cleaning threshold cell
                #

                cleaning_threshold_label = 'cleaning threshold'

                if 'VLASS' in r.imaging_mode:
                    if r.threshold:
                        threshold_quantity = utils.get_casa_quantity(r.threshold)
                        row_cleaning_threshold = '%.2g %s' % (threshold_quantity['value'], threshold_quantity['unit'])
                    else:
                        row_cleaning_threshold = '-'
                elif 'VLA' in r.imaging_mode:
                    cleaning_threshold_label = None
                    row_cleaning_threshold = '-'
                else:
                    if r.threshold:
                        threshold_quantity = qaTool.convert(r.threshold, 'Jy')
                        sp_str, sp_scale = utils.get_si_prefix(threshold_quantity['value'], lztol=1)
                        row_cleaning_threshold = '{:.2g} {}'.format(
                            threshold_quantity['value']/sp_scale, sp_str+brightness_unit)
                        if r.dirty_dynamic_range:
                            row_cleaning_threshold += '<br>Dirty DR: %.2g' % r.dirty_dynamic_range
                            row_cleaning_threshold += '<br>DR correction: %.2g' % r.DR_correction_factor
                        else:
                            row_cleaning_threshold += '<br>No DR information'
                    else:
                        row_cleaning_threshold = '-'

                #
                # nsigma * initial and final scaled MAD for residual image, See PIPE-488
                #
                nsigma_final = r.iterations[maxiter]['imaging_params']['nsigma']

                #
                # heading for non-pbcor RMS cell
                #
                if nchan is None:
                    non_pbcor_label = 'No RMS information'
                elif nchan == 1:
                    non_pbcor_label = 'non-pbcor image RMS'
                else:
                    non_pbcor_label = 'non-pbcor image RMS / RMS<sub>min</sub> / RMS<sub>max</sub>'

                #
                # value for non-pbcor RMS cell
                #
                if nchan is None or r.image_rms is None:
                    row_non_pbcor = '-'
                else:
                    if stokes_parameters != ['I']:
                        r_image_rms = r.image_rms_iquv[stokes_indices[pol]]
                        r_image_rms_max = r.image_rms_iquv[stokes_indices[pol]]
                        r_image_rms_min = r.image_rms_iquv[stokes_indices[pol]]
                    else:
                        r_image_rms = r.image_rms
                        r_image_rms_max = r.image_rms_max
                        r_image_rms_min = r.image_rms_min
                    sp_str, sp_scale = utils.get_si_prefix(image_rms, lztol=1)
                    if nchan == 1:
                        row_non_pbcor = '{:.2g} {}'.format(r_image_rms/sp_scale, sp_str+brightness_unit)
                    else:
                        row_non_pbcor = '{:.2g} / {:.2g} / {:.2g} {}'.format(
                            r_image_rms/sp_scale, r_image_rms_min/sp_scale, r_image_rms_max/sp_scale, sp_str+brightness_unit)

                #
                # un-pbcor image max / min cell
                #
                if r.image_max is None or r.image_min is None:
                    row_pbcor = '-'
                else:
                    sp_str, sp_scale = utils.get_si_prefix(image_max, lztol=0)
                    row_pbcor = '{:.3g} / {:.3g} {}'.format(image_max/sp_scale,
                                                            image_min/sp_scale, sp_str+brightness_unit)

                #
                # fractional bandwidth calculation
                #
                try:
                    frequency1 = summary['refval'][frequency_axis] + \
                        (-0.5 - summary['refpix'][frequency_axis]) * summary['incr'][frequency_axis]
                    frequency2 = summary['refval'][frequency_axis] + (
                        summary['shape'][frequency_axis] - 0.5 - summary['refpix'][frequency_axis]) * summary['incr'][frequency_axis]
                    # full_bw_GHz = qaTool.convert(abs(frequency2 - frequency1), 'GHz')['value']
                    fractional_bw = (frequency2 - frequency1) / (0.5 * (frequency1 + frequency2))
                    fractional_bandwidth = '%.2g%%' % (fractional_bw * 100.)
                except:
                    fractional_bandwidth = 'N/A'

                #
                # fractional bandwidth heading and value
                #
                nterms = r.multiterm if r.multiterm else 1
                if nchan is None:
                    row_fractional_bw_label = 'No channel / width information'
                    row_fractional_bw = '-'
                elif nchan > 1:
                    row_fractional_bw_label = 'channels'
                    if r.orig_specmode == 'repBW':
                        row_fractional_bw = '%d x %s (repBW, LSRK)' % (nchan, width)
                    else:
                        row_fractional_bw = '%d x %s (LSRK)' % (nchan, width)
                else:
                    row_fractional_bw_label = 'fractional bandwidth / nterms'
                    row_fractional_bw = '%s / %s' % (fractional_bandwidth, nterms)

                #
                # aggregate bandwidth heading
                #
                if nchan == 1:
                    row_bandwidth_label = 'aggregate bandwidth'
                else:
                    row_bandwidth_label = None

                #
                # aggregate bandwidth value
                #
                aggregate_bw_GHz = qaTool.convert(r.aggregate_bw, 'GHz')['value']
                row_aggregate_bw = '%.3g GHz (LSRK)' % aggregate_bw_GHz
                row_aggregate_bw_num = '%.4g' % aggregate_bw_GHz

                #
                # VLA statistics (PIPE-764)
                #
                initial_nsigma_mad_label = None
                final_nsigma_mad_label = None

                if 'VLA' in r.imaging_mode:   # VLA and VLASS
                    initial_nsigma_mad_label = 'n-sigma * initial scaled MAD of residual'
                    final_nsigma_mad_label = 'n-sigma * final scaled MAD of residual'

                nsigma_label = None
                row_nsigma = None
                vis_amp_ratio_label = None
                row_vis_amp_ratio = None

                if 'VLA' == r.imaging_mode:  # VLA only
                    nsigma_label = 'nsigma'
                    row_nsigma = nsigma_final
                    vis_amp_ratio_label = 'vis. amp. ratio'
                    row_vis_amp_ratio = r.bl_ratio

                #
                #  score value
                #
                if r.qa.representative is not None:
                    badge_class = rendererutils.get_badge_class(r.qa.representative)
                    row_score = '<span class="badge %s">%0.2f</span>' % (badge_class, r.qa.representative.score)
                else:
                    row_score = '-'

                #
                # check source fit parameters
                #
                if r.check_source_fit is not None:
                    try:
                        chk_pos_offset = '%.2f +/- %.2f' % (r.check_source_fit['offset'],
                                                            r.check_source_fit['offset_err'])
                    except:
                        chk_pos_offset = 'N/A'
                    try:
                        chk_frac_beam_offset = '%.2f +/- %.3f' % (
                            r.check_source_fit['beams'], r.check_source_fit['beams_err'])
                    except:
                        chk_frac_beam_offset = 'N/A'
                    try:
                        chk_fitflux = '%d +/- %d' % (int(utils.round_half_up(r.check_source_fit['fitflux'] * 1000.)), int(
                            utils.round_half_up(r.check_source_fit['fitflux_err'] * 1000.)))
                    except:
                        chk_fitflux = 'N/A'

                    if r.check_source_fit['fitflux'] != 0.0:
                        try:
                            chk_fitpeak_fitflux_ratio = '%.2f' % (
                                r.check_source_fit['fitpeak'] / r.check_source_fit['fitflux'])
                        except:
                            chk_fitpeak_fitflux_ratio = 'N/A'
                    else:
                        chk_fitpeak_fitflux_ratio = 'N/A'

                    if r.check_source_fit['gfluxscale'] is not None and r.check_source_fit['gfluxscale_err'] is not None:
                        try:
                            chk_gfluxscale = '%.2f +/- %.2f' % (
                                r.check_source_fit['gfluxscale'], r.check_source_fit['gfluxscale_err'])
                        except:
                            chk_gfluxscale = 'N/A'

                        if r.check_source_fit['gfluxscale_err'] != 0.0:
                            try:
                                chk_gfluxscale_snr = '%.2f' % (
                                    r.check_source_fit['gfluxscale'] / r.check_source_fit['gfluxscale_err'])
                            except:
                                chk_gfluxscale_snr = 'N/A'
                        else:
                            chk_gfluxscale_snr = 'N/A'

                        if r.check_source_fit['gfluxscale'] != 0.0:
                            try:
                                chk_fitflux_gfluxscale_ratio = '%.2f' % (
                                    r.check_source_fit['fitflux'] * 1000. / r.check_source_fit['gfluxscale'])
                            except:
                                chk_fitflux_gfluxscale_ratio = 'N/A'
                        else:
                            chk_fitflux_gfluxscale_ratio = 'N/A'

                    else:
                        chk_gfluxscale = 'N/A'
                        chk_gfluxscale_snr = 'N/A'
                        chk_fitflux_gfluxscale_ratio = 'N/A'
                else:
                    chk_pos_offset = 'N/A'
                    chk_frac_beam_offset = 'N/A'
                    chk_fitflux = 'N/A'
                    chk_fitpeak_fitflux_ratio = 'N/A'
                    chk_gfluxscale = 'N/A'
                    chk_gfluxscale_snr = 'N/A'
                    chk_fitflux_gfluxscale_ratio = 'N/A'

                #
                # Polarization calibrator fit parameters
                #
                if r.polcal_fit is not None:
                    have_polcal_fit = True
                    pol_session = r.polcal_fit['session']
                    if r.polcal_fit['pol_ratio'] != 'N/A':
                        pol_ratio = f"{qaTool.getvalue(r.polcal_fit['pol_ratio'])[0]:6.2f} +/- {qaTool.getvalue(r.polcal_fit['err_pol_ratio'])[0]:6.2f}%"
                    else:
                        pol_ratio = 'N/A'
                    if r.polcal_fit['pol_angle'] != 'N/A':
                        pol_angle = f"{qaTool.getvalue(r.polcal_fit['pol_angle'])[0]:7.2f} +/- {qaTool.getvalue(r.polcal_fit['err_pol_angle'])[0]:7.2f} deg"
                    else:
                        pol_angle = 'N/A'
                else:
                    pol_session = 'N/A'
                    pol_ratio = 'N/A'
                    pol_angle = 'N/A'

                if r.image_max is not None and r.image_rms is not None:
                    if stokes_parameters != ['I']:
                        r_image_max = r.image_max_iquv[stokes_indices[pol]]
                        r_image_rms = r.image_rms_iquv[stokes_indices[pol]]
                    else:
                        r_image_max = r.image_max
                        r_image_rms = r.image_rms
                    try:
                        img_snr = '%.2f' % (r.image_max / r.image_rms)
                    except:
                        img_snr = 'N/A'
                else:
                    img_snr = 'N/A'

                cube_all_cont = r.cube_all_cont

                tclean_command = r.tclean_command

                # create our table row for this image.
                # Plot is set to None as we have a circular dependency: the row
                # needs the plot, but the plot generator needs the image_stats
                # cache. We will later set plot to the correct value.

                # dirty image statistics (iter 0)

                with casa_tools.ImagepolReader(r.iterations[0]['residual'] + extension) as residualpol:
                    residual = residualpol.stokes(pol)
                    initial_residual_stats = residual.statistics(robust=True)
                    residual.close()

                initial_nsigma_mad = nsigma_final * initial_residual_stats.get('medabsdevmed')[0] * 1.4826
                final_nsigma_mad = nsigma_final * residual_stats.get('medabsdevmed')[0] * 1.4826

                if (nsigma_final != 0.0):
                    #row_initial_nsigma_mad = '%#.3g %s' % (initial_nsigma_mad, brightness_unit)
                    sp_str, sp_scale = utils.get_si_prefix(initial_nsigma_mad, lztol=1)
                    row_initial_nsigma_mad = '{:.2g} {}'.format(initial_nsigma_mad/sp_scale, sp_str+brightness_unit)
                    #row_final_nsigma_mad = '%#.3g %s' % (final_nsigma_mad, brightness_unit)
                    sp_str, sp_scale = utils.get_si_prefix(final_nsigma_mad, lztol=1)
                    row_final_nsigma_mad = '{:.2g} {}'.format(final_nsigma_mad/sp_scale, sp_str+brightness_unit)
                else:
                    row_initial_nsigma_mad = '-'
                    row_final_nsigma_mad = '-'

                # store values in log file
                LOG.info('n-sigma * initial scaled MAD of residual: %s %s' % (("%.12f" % initial_nsigma_mad, brightness_unit)
                                                                              if row_initial_nsigma_mad != '-'
                                                                              else (row_initial_nsigma_mad, "")))
                LOG.info('n-sigma * final scaled MAD of residual: %s %s' % (("%.12f" % final_nsigma_mad, brightness_unit)
                                                                            if row_final_nsigma_mad != '-'
                                                                            else (row_final_nsigma_mad, "")))

                row = ImageRow(
                    vis=vis,
                    datatype=datatype,
                    datatype_info=datatype_info,
                    field=field,
                    fieldname=fieldname,
                    intent=intent,
                    spw=spw,
                    spwnames=spwnames,
                    pol=pol,
                    stokes_label=None,
                    frequency_label=row_frequency_label,
                    frequency=row_frequency,
                    beam=row_beam,
                    beam_pa=row_beam_pa,
                    sensitivity=row_sensitivity,
                    cleaning_threshold_label=cleaning_threshold_label,
                    cleaning_threshold=row_cleaning_threshold,
                    initial_nsigma_mad_label=initial_nsigma_mad_label,
                    initial_nsigma_mad=row_initial_nsigma_mad,
                    final_nsigma_mad_label=final_nsigma_mad_label,
                    final_nsigma_mad=row_final_nsigma_mad,
                    model_pos_flux=row_model_pos_flux,
                    model_neg_flux=row_model_neg_flux,
                    model_flux_inner_deg=row_model_flux_inner_deg,
                    nmajordone_total=row_nmajordone_total,
                    nmajordone_per_iter=row_nmajordone_per_iter,
                    majorcycle_stat_plot=majorcycle_stat_plot,
                    tab_dict=tab_dict,
                    tab_url=None,
                    residual_ratio=row_residual_ratio,
                    non_pbcor_label=non_pbcor_label,
                    non_pbcor=row_non_pbcor,
                    pbcor=row_pbcor,
                    score=row_score,
                    fractional_bw_label=row_fractional_bw_label,
                    fractional_bw=row_fractional_bw,
                    aggregate_bw_label=row_bandwidth_label,
                    aggregate_bw=row_aggregate_bw,
                    aggregate_bw_num=row_aggregate_bw_num,
                    nsigma_label=nsigma_label,
                    nsigma=row_nsigma,
                    vis_amp_ratio_label=vis_amp_ratio_label,
                    vis_amp_ratio=row_vis_amp_ratio,
                    image_file=image_name.replace('.pbcor', ''),
                    nchan=nchan,
                    plot=None,
                    qa_url=None,
                    outmaskratio=row_outmaskratio,
                    outmaskratio_label=row_outmaskratio_label,
                    iterdone=row_iterdone,
                    stopcode=row_stopcode,
                    stopreason=row_stopreason,
                    chk_pos_offset=chk_pos_offset,
                    chk_frac_beam_offset=chk_frac_beam_offset,
                    chk_fitflux=chk_fitflux,
                    chk_fitpeak_fitflux_ratio=chk_fitpeak_fitflux_ratio,
                    img_snr=img_snr,
                    chk_gfluxscale=chk_gfluxscale,
                    chk_gfluxscale_snr=chk_gfluxscale_snr,
                    chk_fitflux_gfluxscale_ratio=chk_fitflux_gfluxscale_ratio,
                    cube_all_cont=cube_all_cont,
                    tclean_command=tclean_command,
                    pol_session=pol_session,
                    pol_ratio=pol_ratio,
                    pol_angle=pol_angle,
                    poli_abspath = None,
                    poli_thumbnail = None,
                    pola_abspath = None,
                    pola_thumbnail = None,
                    result=r
                )
                image_rows.append(row)

        # PIPE-2191/PIPE-2022: create and attach a warning-level logging handler with a content filter only capturing
        # png-missing warning messages.
        extra_logrecords_handler = logging.CapturingHandler(logging.WARNING)
        extra_logrecords_handler.addFilter(missing_png_filter)
        logging.add_handler(extra_logrecords_handler)

        try:
            plotter = display.CleanSummary(context, makeimages_result, image_stats)
            plots = plotter.plot()

            plots_dict = make_plot_dict(plots)

            # construct the renderers so we know what the back/forward links will be
            # sort the rows so the links will be in the same order as the rows
            image_rows.sort(key=lambda row: (row.image_file.split(
                '.')[0], row.datatype, row.field, utils.natural_sort_key(row.spw), row.pol))
            temp_urls = (None, None, None)
            qa_renderers = [TCleanPlotsRenderer(context, results, row.result, plots_dict, row.image_file.split(
                '.')[0], row.field, str(row.spw), row.pol, row.datatype, temp_urls, row.cube_all_cont) for row in image_rows]
            qa_links = triadwise([renderer.path for renderer in qa_renderers])

            # PIPE-991: render tclean major cycle table, but only if tab_dict is specified (currently VLASS-SE-CONT)
            tab_renderer = [TCleanTablesRenderer(context, results, row.result,
                                                 row.tab_dict, row.image_file.split('.')[0], row.field, str(row.spw),
                                                 row.pol, temp_urls) if row.tab_dict else None for row in image_rows]
            tab_links = triadwise([renderer.path if renderer else None for renderer in tab_renderer])

            final_rows = []

            for row, renderer, qa_urls, tab_url in zip(image_rows, qa_renderers, qa_links, tab_links):
                prefix = row.image_file.split('.')[0]

                try:
                    final_iter = sorted(plots_dict[prefix][row.datatype][row.field][str(row.spw)][str(row.pol)].keys())[-1]

                    # cube and repBW mode use mom8
                    plot = get_plot(plots_dict, prefix, row.datatype, row.field, str(row.spw), row.pol, final_iter, 'image', 'mom8')
                    if plot is None:
                        # mfs and cont mode use mean
                        plot = get_plot(plots_dict, prefix, row.datatype, row.field, str(row.spw), row.pol, final_iter, 'image', 'mean')

                    renderer = TCleanPlotsRenderer(context, results, row.result,
                                                   plots_dict, prefix, row.field, str(row.spw), row.pol,
                                                   row.datatype, qa_urls, row.cube_all_cont)
                    with renderer.get_file() as fileobj:
                        fileobj.write(renderer.render())

                    values = row._asdict()
                    values['plot'] = plot
                    values['qa_url'] = renderer.path

                    # PIPE-991: render tclean major cycle table, but only if tab_dict exists (currently VLASS-SE-CONT)
                    if any(tab_url):
                        tab_renderer = TCleanTablesRenderer(context, results, row.result,
                                                            row.tab_dict, prefix, row.field, str(row.spw), row.pol,
                                                            tab_url)
                        with tab_renderer.get_file() as fileobj:
                            fileobj.write(tab_renderer.render())
                        values['tab_url'] = tab_renderer.path

                    # Save POLI/POLA paths which is known only after plot() has been called
                    pol_plot = get_plot(plots_dict, prefix, row.datatype, row.field, str(row.spw), 'Ptotal', final_iter, 'image', 'mean')
                    if pol_plot is not None:
                        values['poli_abspath'] = pol_plot.abspath
                        values['poli_thumbnail'] = pol_plot.thumbnail
                    pol_plot = get_plot(plots_dict, prefix, row.datatype, row.field, str(row.spw), 'Pangle', final_iter, 'image', 'mean')
                    if pol_plot is not None:
                        values['pola_abspath'] = pol_plot.abspath
                        values['pola_thumbnail'] = pol_plot.thumbnail

                    new_row = ImageRow(**values)
                    final_rows.append(new_row)
                except IOError as e:
                    LOG.error(e)
                    LOG.error(traceback.format_exc())
                except Exception as e:
                    # Probably some detail page rendering exception.
                    LOG.error(e)
                    LOG.error(traceback.format_exc())
                    final_rows.append(row)

            # primary sort images by vis, datatype, field, secondary sort on spw, then by pol
            final_rows.sort(key=lambda row: (row.vis, row.datatype, row.field, utils.natural_sort_key(row.spw), row.pol))

            chk_fit_rows = []
            for row in final_rows:
                if row.frequency is not None:
                    chk_fit_rows.append((row.vis, row.fieldname, row.spw, row.aggregate_bw_num, row.chk_pos_offset, row.chk_frac_beam_offset, row.chk_fitflux,
                                        row.img_snr, row.chk_fitpeak_fitflux_ratio, row.chk_gfluxscale, row.chk_gfluxscale_snr, row.chk_fitflux_gfluxscale_ratio))
            chk_fit_rows = utils.merge_td_columns(chk_fit_rows, num_to_merge=2)

            pol_fit_rows = []
            pol_fit_plots = []
            for row in final_rows:
                if row.pol == 'I':
                    # Save only once for weblog because the fit is the same for all Stokes parameters
                    pol_fit_rows.append((row.pol_session, row.vis, row.fieldname, row.spw, row.pol_ratio, row.pol_angle))
                    pol_fit_plots.append(PolImagePaths(poli_abspath=row.poli_abspath,
                                                       poli_thumbnail=row.poli_thumbnail,
                                                       pola_abspath=row.pola_abspath,
                                                       pola_thumbnail=row.pola_thumbnail))
            pol_fit_rows = utils.merge_td_columns(pol_fit_rows, num_to_merge=4)

            vlass_cubesummary_plots = display.VlassCubeSummary(context, makeimages_result).plot()

            if vlass_cubesummary_plots:
                vlass_cubesummary_plots_html = plots_to_html(vlass_cubesummary_plots, report_dir=context.report_dir)[0]
            else:
                vlass_cubesummary_plots_html = None

            plane_keep = makeimages_result.metadata['vlass_cube_metadata']['plane_keep']
            spwgroup_list = makeimages_result.metadata['vlass_cube_metadata']['spwgroup_list']
            plane_keep_dict = {spwgroup: plane_keep[idx] for idx, spwgroup in enumerate(spwgroup_list)}

        finally:
            # PIPE-2191/PIPE-2022: remove the local logging handler and attach the LogRecord list to extra_logrecords.
            logging.remove_handler(extra_logrecords_handler)
            extra_logrecords = extra_logrecords_handler.buffer

        ctx.update({
            'plots': plots,
            'plots_dict': plots_dict,
            'image_info': final_rows,
            'dirname': weblog_dir,
            'vlass_cubesummary_plots_html': vlass_cubesummary_plots_html,
            'plane_keep_dict': plane_keep_dict,
            'extra_logrecords': extra_logrecords
        })


def get_cycle_stats(context, makeimages_result, r):
    """Get the major cycle statistics."""

    row_nmajordone_per_iter, row_nmajordone_total, majorcycle_stat_plot, tab_dict = None, None, None, None

    if 'VLASS-SE-CUBE' in r.imaging_mode or 'VLASS-SE-CONT' in r.imaging_mode:

        # collect the major/minor cycle stats for each CASA/tclean call (i.e. each 'iter' of Tclean)
        row_nmajordone_per_iter = {}
        for iteration, iterdata in r.iterations.items():
            iter_dict = {'cleanmask': iterdata['cleanmask'] if 'cleanmask' in iterdata else '',
                         'nmajordone': iterdata['nmajordone'] if 'nmajordone' in iterdata else 0,
                         'nminordone_array': None,
                         'peakresidual_array': None,
                         'totalflux_array': None,
                         'planeid_array': None}
            if 'summaryminor' in iterdata:
                # after CAS-6692
                # note: For MPI runs, one must set the env variable "USE_SMALL_SUMMARYMINOR" to True (see CAS-6692),
                # or use the proposed new CASA/tclean parameter summary='full' (see CAS-13924).
                field_id, channel_id = 0, 0  # explictly assume one imaging field & one channel (valid for VLASS)
                summaryminor = iterdata['summaryminor'][field_id][channel_id]
                iter_dict['nminordone_array'] = np.asarray([ss for s in summaryminor.values()
                                                           for sn in zip(s['startIterDone'], s['iterDone']) for ss in [sn[0], sn[0] + sn[1]]])
                iter_dict['peakresidual_array'] = np.asarray([ss
                                                              for s in summaryminor.values() for sn in zip(s['startPeakRes'],
                                                                                                           s['peakRes']) for ss in sn])
                iter_dict['totalflux_array'] = np.asarray([ss for s in summaryminor.values()
                                                          for sn in zip(s['startModelFlux'], s['modelFlux']) for ss in sn])
                iter_dict['planeid_array'] = np.asarray([pp for p in summaryminor for pp in [p]*len(summaryminor[p]['iterDone'])*2])
            else:
                # before CAS-6692
                iter_dict['nminordone_array'] = iterdata['nminordone_array'] if 'nminordone_array' in iterdata else None
                iter_dict['peakresidual_array'] = iterdata['peakresidual_array'] if 'peakresidual_array' in iterdata else None
                iter_dict['totalflux_array'] = iterdata['totalflux_array'] if 'totalflux_array' in iterdata else None
                iter_dict['planeid_array'] = iterdata['planeid_array'] if 'planeid_array' in iterdata else None
            row_nmajordone_per_iter[iteration] = iter_dict

        # sum the major cycle done over all 'iter's of Tclean
        row_nmajordone_total = np.sum([item['nmajordone'] for key, item in row_nmajordone_per_iter.items()])

        # generate the major cycle stats summary plot
        majorcycle_stat_plot = display.TcleanMajorCycleSummaryFigure(
            context, makeimages_result, row_nmajordone_per_iter, figname=r.psf.replace('.psf', '')).plot()

        # collect info for the major cycle stats summary table
        tab_dict = {0: {'cols': ['iteration', 'cleanmask', 'nmajordone'],
                        'nrow': len(row_nmajordone_per_iter),
                        'iteration': [k for k in row_nmajordone_per_iter],
                        'cleanmask': [item['cleanmask'] for iter, item in row_nmajordone_per_iter.items()],
                        'nmajordone': [item['nmajordone'] for iter, item in row_nmajordone_per_iter.items()]}}
    else:
        # Just nmajor for other modes
        row_nmajordone_per_iter = {}
        for iteration, iterdata in r.iterations.items():
            iter_dict = {'nmajordone': iterdata['nmajordone'] if 'nmajordone' in iterdata else 0}
            row_nmajordone_per_iter[iteration] = iter_dict

        # sum the major cycle done over all 'iter's of Tclean
        row_nmajordone_total = np.sum([item['nmajordone'] for key, item in row_nmajordone_per_iter.items()])

    return row_nmajordone_per_iter, row_nmajordone_total, majorcycle_stat_plot, tab_dict


def missing_png_filter(record):
    """Filter log records for messages indicating missing PNG files on disk.

    This function is designed to be used as a filter for logging handlers. It checks
    if the log message matches a pattern indicating that a PNG file is missing on disk.

    Args:
        record (logging.LogRecord): The log record to be filtered.

    Returns:
        bool: True if the log message indicates a missing PNG file, False otherwise.

    Example:
        handler = logging.Handler()
        handler.addFilter(missing_png_filter)
    """
    return fnmatch.fnmatch(record.getMessage(), '*is missing on disk*')
