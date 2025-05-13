import ast
import os
import re
import shutil
from copy import deepcopy
from math import factorial
import collections

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


# old
#  scipy.signal.savgol_filter(x, window_length, polyorder, deriv=0, delta=1.0, axis=-1, mode='interp', cval=0.0)

# http://scipy-cookbook.readthedocs.io/items/SavitzkyGolay.html
def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    r"""Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    The Savitzky-Golay filter removes high frequency noise from data.
    It has the advantage of preserving the original shape and
    features of the signal better than other types of filtering
    approaches, such as moving averages techniques.
    Parameters
    ----------
    y : array_like, shape (N,)
        the values of the time history of the signal.
    window_size : int
        the length of the window. Must be an odd integer number.
    order : int
        the order of the polynomial used in the filtering.
        Must be less then `window_size` - 1.
    deriv: int
        the order of the derivative to compute (default = 0 means only smoothing)
    Returns
    -------
    ys : ndarray, shape (N)
        the smoothed signal (or it's n-th derivative).
    Notes
    -----
    The Savitzky-Golay is a type of low-pass filter, particularly
    suited for smoothing noisy data. The main idea behind this
    approach is to make for each point a least-square fit with a
    polynomial of high order over a odd-sized window centered at
    the point.
    Examples
    --------
    t = np.linspace(-4, 4, 500)
    y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    ysg = savitzky_golay(y, window_size=31, order=4)
    import matplotlib.pyplot as plt
    plt.plot(t, y, label='Noisy signal')
    plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    plt.plot(t, ysg, 'r', label='Filtered signal')
    plt.legend()
    plt.show()
    References
    ----------
    .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
       Data by Simplified Least Squares Procedures. Analytical
       Chemistry, 1964, 36 (8), pp 1627-1639.
    .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
       W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
       Cambridge University Press ISBN-13: 9780521880688
    """
    try:
        window_size = np.abs(int(window_size))
        order = np.abs(int(order))
    except ValueError:
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")

    half_window = (window_size - 1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in range(order + 1)] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs(y[1:half_window+1][::-1] - y[0])
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))

    return np.convolve( m[::-1], y, mode='valid')


class SyspowerResults(basetask.Results):
    def __init__(self, gaintable=None, spowerdict=None, dat_common=None,
                 clip_sp_template=None, template_table=None, band_baseband_spw=None, plotrq=None):

        if gaintable is None:
            gaintable = ''
        if spowerdict is None:
            spowerdict = {}
        if dat_common is None:
            dat_common = np.array([])
        if clip_sp_template is None:
            clip_sp_template = []
        if template_table is None:
            template_table = ''
        if band_baseband_spw is None:
            band_baseband_spw = collections.defaultdict(dict)
        if plotrq is None:
            plotrq = ''

        super(SyspowerResults, self).__init__()

        self.pipeline_casa_task = 'Syspower'
        self.gaintable = gaintable
        self.spowerdict = spowerdict
        self.dat_common = dat_common
        self.clip_sp_template = clip_sp_template
        self.template_table = template_table
        self.band_baseband_spw = band_baseband_spw
        self.plotrq = plotrq

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        # return 'SyspowerResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'SyspowerResults:'


class SyspowerInputs(vdp.StandardInputs):
    antexclude = vdp.VisDependentProperty(default={})
    apply = vdp.VisDependentProperty(default=False)
    do_not_apply = vdp.VisDependentProperty(default='')

    @vdp.VisDependentProperty
    def clip_sp_template(self):
        return [0.7, 1.2]


    def __init__(self, context, vis=None, clip_sp_template=None, antexclude=None,
                 apply=None, do_not_apply=None):
        self.context = context
        self.vis = vis
        self.clip_sp_template = clip_sp_template
        self.antexclude = antexclude
        self.apply = apply
        self.do_not_apply = do_not_apply


@task_registry.set_equivalent_casa_task('hifv_syspower')
@task_registry.set_casa_commands_comment('Sys power fix compression')
class Syspower(basetask.StandardTaskTemplate):
    Inputs = SyspowerInputs

    def prepare(self):
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        # flag normalized p_diff outside this range
        clip_sp_template = self.inputs.clip_sp_template
        if isinstance(self.inputs.clip_sp_template, str):
            clip_sp_template = ast.literal_eval(self.inputs.clip_sp_template)

        antexclude_dict = {}

        if isinstance(self.inputs.antexclude, str):
            antexclude_dict = ast.literal_eval(self.inputs.antexclude)
        elif isinstance(self.inputs.antexclude, dict):
            antexclude_dict = self.inputs.antexclude

        # Assumes hifv_priorcals was executed as the previous stage
        try:
            rq_table = self.inputs.context.results[-1].read()[0].rq_result[0].final[0].gaintable
        except Exception as ex:
            rq_table = self.inputs.context.results[-1].read()[0].rq_result.final[0].gaintable
            LOG.debug(ex)

        band_baseband_spw = collections.defaultdict(dict)

        # Look for flux cal
        fields = m.get_fields(intent='AMPLITUDE')

        # No amp cal - look for bandpass
        if not fields:
            fields = m.get_fields(intent='BANDPASS')
            LOG.error("Unable to identify field with intent='AMPLITUDE'.  Trying BANDPASS calibrator.")

        # Exit if no amp or bp
        if not fields:
            LOG.error("No AMPLITUDE or BANDPASS intents found.  Exiting task.")
            return SyspowerResults(gaintable=rq_table, spowerdict={}, dat_common=None,
                                   clip_sp_template=None, template_table=None,
                                   band_baseband_spw=band_baseband_spw)

        field = fields[0]
        flux_field = field.id
        flux_times = field.time
        LOG.info("Using field: {0}  (ID: {1})".format(field.name, flux_field))

        antenna_ids = np.array([a.id for a in m.antennas])
        antenna_names = [a.name for a in m.antennas]
        # spws = [spw.id for spw in m.get_spectral_windows(science_windows_only=True)]

        # Determine if 8-bit or 3-bit
        # 8-bit continuum applications (GHz)      3-bit continuum applications (GHz)
        # IF pair A0/C0   IF pair B0/D0           IF pair A1C1    IF pair A2C2    IF pair B1D1    IF pair B2D2

        allowedbasebands = ('A0C0', 'B0D0', 'A1C1', 'A2C2', 'B1D1', 'B2D2')
        allowed_rcvr_bands = ['L', 'S', 'C', 'X', 'KU', 'K', 'KA', 'Q']

        # create a band exclusion list
        if self.inputs.do_not_apply:
            do_not_apply_list = self.inputs.do_not_apply.split(',')
        else:
            do_not_apply_list = []
        for no_band in do_not_apply_list:
            LOG.warning("Keyword override - will not apply {!s}-band".format(no_band))

        banddict = m.get_vla_baseband_spws(science_windows_only=True, return_select_list=False, warning=False)
        allprocessedspws = []
        do_not_apply_spws = []

        for band in banddict:
            baseband2spw = {}
            for baseband in banddict[band]:
                if baseband in allowedbasebands and band in allowed_rcvr_bands:
                    spwsperbaseband = []
                    for spwdict in banddict[band][baseband]:
                        for spw, value in spwdict.items():
                            spwsperbaseband.append(spw)
                    allprocessedspws.extend(spwsperbaseband)
                    if band in do_not_apply_list:
                        do_not_apply_spws.extend(spwsperbaseband)
                    baseband2spw[baseband] = spwsperbaseband
                    band_baseband_spw[band] = baseband2spw

        if not band_baseband_spw:
            LOG.info("No bands/basebands in these data will be processed for the hifv_syspower task.")
            return SyspowerResults(gaintable=rq_table, spowerdict={}, dat_common=None,
                                   clip_sp_template=None, template_table=None,
                                   band_baseband_spw=band_baseband_spw)

        LOG.debug('----------------------------------')
        LOG.debug(band_baseband_spw)
        LOG.debug('----------------------------------')

        # Execute for each band, and then run the code normally for each baseband within a given band.
        spowerdict = {}
        template_table = {}
        dat_common_final = {}

        # Make a copy of the RQ table and operate on that copy
        # We need to apply the changes and create the diff tables for plotting purposes,
        #   whether or not apply is True/False
        temprq = 'rq_temp.tbl'
        if os.path.isdir(temprq):
            shutil.rmtree(temprq)
        shutil.copytree(rq_table, temprq)

        for band in band_baseband_spw:
            LOG.info('-----------------------------------')
            LOG.info('Processing syspower {!s}-band...'.format(band))

            '''
            # Example dictionary format of the task keyword antexclude
            {'L': {'ea02': {'usemedian': True}, 'ea03': {'usemedian': False}},
             'X': {'ea02': {'usemedian': True}, 'ea03': {'usemedian': False}},
             'S': {'ea12': {'usemedian': False}, 'ea22': {'usemedian': False}}}
            '''
            antexclude = ''
            usemedian_perant = []
            usemedian_perant_dict = {}

            if self.inputs.antexclude:
                if band in antexclude_dict.keys():
                    antexclude_list = list(antexclude_dict[band].keys())
                    antexclude = ','.join(antexclude_list)
                    usemedian_perant = [i['usemedian'] for i in list(antexclude_dict[band].values())]
                    # usemedian_perant = antband_exclude[band]['usemedian']
                    usemedian_perant_dict = dict(zip(antexclude_list, usemedian_perant))

            spws = []
            for baseband in band_baseband_spw[band]:
                spws.extend(band_baseband_spw[band][baseband])

            template_table_band = 'pdiff_{!s}.tbl'.format(band)

            # get syspower from MS
            with casa_tools.TableReader(self.inputs.vis + '/SYSPOWER') as tb:
                # stb = tb.query('SPECTRAL_WINDOW_ID > '+str(min(spws)-1))  # VLASS specific for offset of two spws?
                stb = tb.query('SPECTRAL_WINDOW_ID in [{!s}]'.format(','.join([str(spw) for spw in spws])))
                sp_time = stb.getcol('TIME')
                sp_ant = stb.getcol('ANTENNA_ID')
                sp_spw = stb.getcol('SPECTRAL_WINDOW_ID')
                p_diff = stb.getcol('SWITCHED_DIFF')
                rq = stb.getcol('REQUANTIZER_GAIN')
                stb.done()

            # setup arrays
            sorted_time      = np.unique(sp_time)
            dat_raw          = np.zeros((len(antenna_ids), len(spws), 2, len(sorted_time)))
            dat_rq           = np.zeros((len(antenna_ids), len(spws), 2, len(sorted_time)))
            dat_flux         = np.zeros((len(antenna_ids), len(spws), 2))
            dat_scaled       = np.zeros((len(antenna_ids), len(spws), 2, len(sorted_time)))
            dat_filtered     = np.zeros((len(antenna_ids), len(spws), 2, len(sorted_time)))
            # dat_common       = np.ma.zeros((len(antenna_ids), 2, 2, len(sorted_time)))
            dat_online_flags = np.zeros((len(antenna_ids), len(sorted_time)), dtype='bool')
            dat_sum          = np.zeros((len(antenna_ids), len(spws), 2, len(sorted_time)))
            dat_sum_flux     = np.zeros((len(antenna_ids), len(spws), 2, len(sorted_time)))

            # get online flags from .flagonline.txt
            flag_file_name = self.inputs.vis.replace('.ms', '.flagonline.txt')
            if os.path.isfile(flag_file_name):
                with open(flag_file_name, 'r') as flag_file:
                    for line in flag_file:
                        try:
                            r = re.search(r"antenna='ea(\d*)&&\*' timerange='(.*)' reason", line)
                        except Exception as e:
                            r = False
                        if r:
                            this_ant = 'ea' + r.groups()[0]
                            start_time = r.groups()[1].split('~')[0]
                            end_time = r.groups()[1].split('~')[1]
                            start_time_sec = casa_tools.quanta.convert(casa_tools.quanta.quantity(start_time), 's')['value']
                            end_time_sec = casa_tools.quanta.convert(casa_tools.quanta.quantity(end_time), 's')['value']
                            indices_to_flag = np.where((sorted_time >= start_time_sec) & (sorted_time <= end_time_sec))[0]
                            dat_online_flags[antenna_names.index(this_ant), indices_to_flag] = True

            # remove requantizer changes from p_diff
            pdrq = np.ma.masked_invalid(p_diff/(np.ma.masked_equal(rq, 0)**2))

            # read tables into arrays
            # dat_filtered dimensions: (n_ant, n_spw, n_pol, n_hits)
            spw_problems = []
            for i, this_ant in enumerate(antenna_ids):
                LOG.info('reading antenna {0}'.format(this_ant))
                for j, this_spw in enumerate(spws):
                    hits = np.where((sp_ant == this_ant) & (sp_spw == this_spw))[0]
                    times, ind = np.unique(sp_time[hits], return_index=True)
                    hits2 = np.where(np.in1d(sorted_time, times))[0]
                    flux_hits = np.where((times >= np.min(flux_times)) & (times <= np.max(flux_times)))[0]
                    if len(hits) != len(hits2):
                        spw_problems.append(this_spw)
                        if len(ind) == len(hits2):
                            hits = hits[ind]

                    for pol in [0, 1]:
                        LOG.debug(str(i) + ' ' + str(j) + ' ' + str(pol) + ' ' + str(hits2))
                        dat_raw[i, j, pol, hits2] = p_diff[pol, hits]
                        dat_flux[i, j, pol] = np.ma.median(pdrq[pol, hits][flux_hits])
                        dat_rq[i, j, pol, hits2] = rq[pol, hits]
                        dat_scaled[i, j, pol, hits2] = pdrq[pol, hits] / dat_flux[i, j, pol]
                        dat_filtered[i, j, pol, hits2] = deepcopy(dat_scaled[i, j, pol, hits2])

            if spw_problems:
                spw_problems = list(set(spw_problems))
                LOG.warning("Caution - missing or duplicated data - timing issue with the syspower table.  " +
                            "Review the data for spw='{!s}'".format(','.join([str(spw) for spw in spw_problems])))

            # Determine which spws go with which basebands
            # There could be multiple baseband names per band - count them
            bband_common_indices = []
            bbindex = 0
            # for band in band_baseband_spw:
            for baseband in band_baseband_spw[band]:
                if band_baseband_spw[band][baseband]:
                    numspws = len(band_baseband_spw[band][baseband])
                    if bbindex == 0:
                        bband_common_indices.append(list(range(0, numspws)))
                    else:
                        startindex = bband_common_indices[bbindex - 1][-1] + 1
                        bband_common_indices.append(list(range(startindex, startindex + numspws)))
                    bbindex += 1

            LOG.debug('----------------------------------')
            LOG.debug(bband_common_indices)
            LOG.debug('----------------------------------')

            # dat_common for this particular receiver band
            dat_common = np.ma.zeros((len(antenna_ids), bbindex, 2, len(sorted_time)))

            # common baseband template
            for i, this_ant in enumerate(antenna_ids):
                LOG.info('Creating template for antenna {0}'.format(antenna_names[this_ant]))
                for bband in range(bbindex):
                    # common_indices = list(range(0, 8)) if bband == 0 else list(range(8, 16))  # VLASS Specific
                    common_indices = bband_common_indices[bband]
                    for pol in [0, 1]:
                        LOG.info('  processing band {0}, baseband {1},  polarization {2}'.format(band, bband, pol))

                        # create initial template
                        # sp_data dimmensions: (n_spw, n_hits)
                        sp_data = dat_filtered[i, common_indices, pol, :]
                        sp_data = np.ma.array(sp_data)
                        sp_data.mask = np.ma.getmaskarray(sp_data)
                        sp_data.mask = dat_online_flags[i]

                        sp_data, flag_percent = self.flag_with_medfilt(sp_data, sp_data, flag_median=True,
                                                                       k=9, threshold=8, do_shift=True)
                        LOG.info('    total flagged data: {0:.2f}% in first pass'.format(flag_percent))

                        sp_data, flag_percent = self.flag_with_medfilt(sp_data, sp_data, flag_rms=True,
                                                                       k=5, threshold=8, do_shift=True)
                        LOG.info('    total flagged data: {0:.2f}% in second pass'.format(flag_percent))

                        if sp_data.shape[0] > 1:
                            # flag residuals and recalculate template
                            sp_template = np.ma.median(sp_data, axis=0)
                            sp_data, flag_percent = self.flag_with_medfilt(sp_data, sp_template, flag_median=True,
                                                                           k=11, threshold=7, do_shift=False)
                            LOG.info('    total flagged data: {0:.2f}% in third pass'.format(flag_percent))

                            sp_data, flag_percent = self.flag_with_medfilt(sp_data, sp_template, flag_rms=True,
                                                                           k=5, threshold=7, do_shift=False)
                            LOG.info('    total flagged data: {0:.2f}% in fourth pass'.format(flag_percent))

                        sp_median_data = np.ma.median(sp_data, axis=0)
                        sp_median_mask = deepcopy(sp_median_data.mask)
                        # scipy.signal.savgol_filter(x, window_length, polyorder, deriv=0, delta=1.0,
                        #                            axis=-1, mode='interp', cval=0.0) # OLD
                        # savitzky_golay(y, window_size, order, deriv=0, rate=1):  NEW
                        # sp_template = savgol_filter(self.interp_with_medfilt(sp_median_data), 7, 3)
                        sp_template = savitzky_golay(self.interp_with_medfilt(sp_median_data), 7, 3)
                        sp_template = np.ma.array(sp_template)
                        sp_template.mask = np.ma.getmaskarray(sp_template)
                        sp_template.mask = sp_median_mask
                        LOG.info('    restored {0:.2f}% template flags after interpolation'.format(
                                 100.0 * np.sum(sp_median_mask) / sp_median_mask.size))

                        # repeat after square root
                        if isinstance(sp_data.mask, bool):
                            sp_data.mask = np.ma.getmaskarray(sp_data)
                        sp_data.mask[sp_data < 0] = True
                        sp_data = sp_data ** .5
                        sp_template = sp_template ** .5
                        sp_data.mask[sp_data != sp_data] = True
                        sp_data, flag_percent = self.flag_with_medfilt(sp_data, sp_template, flag_rms=True,
                                                                       flag_median=True,
                                                                       k=5, threshold=6, do_shift=False)
                        LOG.info('    total flagged data: {0:.2f}% in fifth pass'.format(flag_percent))
                        sp_median_data = np.ma.median(sp_data, axis=0)
                        # sp_median_mask = deepcopy(sp_median_data.mask)
                        sp_template = savitzky_golay(self.interp_with_medfilt(sp_median_data), 7, 3)

                        dat_common[i, bband, pol, :] = sp_template

            spowerdictband = dict()
            spowerdictband['spower_raw']          = dat_raw
            spowerdictband['spower_flux_levels']  = dat_flux
            spowerdictband['spower_rq']           = dat_rq
            spowerdictband['spower_scaled']       = dat_scaled
            spowerdictband['spower_filtered']     = dat_filtered
            spowerdictband['spower_common']       = np.ma.filled(dat_common, 0)
            spowerdictband['spower_online_flags'] = dat_online_flags
            spowerdictband['spower_sum']          = dat_sum
            spowerdictband['spower_sum_flux']     = dat_sum_flux

            # flag template using clip values
            final_template = np.ma.array(dat_common)
            final_template.mask = np.ma.getmaskarray(final_template)

            final_template.mask[final_template < clip_sp_template[0]] = True
            final_template.mask[final_template > clip_sp_template[1]] = True

            antids = list(antenna_ids)
            if usemedian_perant and antexclude != '':
                for i, this_ant in enumerate(antenna_ids):
                    antindex = antids.index(i)
                    antname = antenna_names[antindex]
                    if antname in antexclude:
                        LOG.info("Antenna " + antname + " to be excluded.")
                        final_template.mask[i, :, :, :] = np.ma.masked  # Change mask values to True for that antenna
                median_final_template = np.ma.median(final_template, axis=0)

            for i, this_ant in enumerate(antenna_ids):
                antindex = antids.index(i)
                antname = antenna_names[antindex]
                if antname in antexclude:
                    usemedian = usemedian_perant_dict[antname]
                    if usemedian:
                        LOG.info("Using median value in template for antenna " + antname + ".")
                        final_template.data[i, :, :, :] = median_final_template.data
                        final_template.mask[i, :, :, :] = median_final_template.mask
                    else:
                        LOG.info("Using value of 1.0 in template for antenna " + antname + ".")
                        final_template.data[i, :, :, :] = 1.0
                        final_template.mask[i, :, :, :] = np.ma.nomask

            with casa_tools.TableReader(temprq, nomodify=False) as tb:
                rq_time = tb.getcol('TIME')
                rq_spw = tb.getcol('SPECTRAL_WINDOW_ID')
                rq_par = tb.getcol('FPARAM')
                rq_ant = tb.getcol('ANTENNA1')
                rq_flag = tb.getcol('FLAG')

                LOG.info('Starting RQ table')
                # spw_offset = 2  # This was hardwired for VLASS in the past
                for i, this_ant in enumerate(antenna_ids):
                    LOG.info('  writing RQ table for antenna {0}'.format(this_ant))

                    # for j, this_spw in enumerate(range(len(spws))):
                    for j, this_spw in enumerate(spws):
                        # hits = np.where((rq_ant == i) & (rq_spw == j + spw_offset))[0]
                        hits = np.where((rq_ant == i) & (rq_spw == this_spw))[0]
                        # bband = 0 if (j < 8) else 1

                        for subarray in bband_common_indices:
                            if j in subarray:
                                bband = bband_common_indices.index(subarray)

                        hits2 = np.where(np.in1d(sorted_time, rq_time[hits]))[0]

                        for pol in [0, 1]:
                            try:
                                rq_par[2 * pol, 0, hits] *= final_template[i, bband, pol, hits2].data
                                rq_flag[2 * pol, 0, hits] = np.logical_or(rq_flag[2 * pol, 0, hits],
                                                                      final_template[i, bband, pol, hits2].mask)
                                if j in [0, 8]:
                                    message = '  {2}% of solutions flagged in band {0}, baseband {1},  polarization {2}'
                                    LOG.info(message.format(band, bband, pol, 100. * np.sum(rq_flag[2 * pol, 0, hits]) /
                                                                                      rq_flag[2 * pol, 0, hits].size))
                            except Exception as e:
                                LOG.warning('Error preparing final RQ table')
                                raise  # SystemExit('shape mismatch writing final RQ table')

                try:

                    tb.putcol('FPARAM', rq_par)
                    tb.putcol('FLAG', rq_flag)
                except Exception as ex:
                    LOG.warning('Error writing final RQ table - switched power will not be applied' + str(ex))

            # create new table to plot pdiff template_table per band
            if os.path.isdir(template_table_band):
                shutil.rmtree(template_table_band)
            shutil.copytree(temprq, template_table_band)

            with casa_tools.TableReader(template_table_band, nomodify=False) as tb:
                for i, this_ant in enumerate(antenna_ids):
                    # for j, this_spw in enumerate(range(len(spws))):
                    for j, this_spw in enumerate(spws):
                        hits = np.where((rq_ant == i) & (rq_spw == this_spw))[0]
                        # bband = 0 if (j < 8) else 1
                        for subarray in bband_common_indices:
                            if j in subarray:
                                bband = bband_common_indices.index(subarray)
                        hits2 = np.where(np.in1d(sorted_time, rq_time[hits]))[0]

                        for pol in [0, 1]:
                            try:
                                rq_par[2 * pol, 0, hits] = final_template[i, bband, pol, hits2].data
                                rq_flag[2 * pol, 0, hits] = final_template[i, bband, pol, hits2].mask
                            except Exception as ex:
                                LOG.error('Shape mismatch writing final template table')

                tb.putcol('FPARAM', rq_par)
                tb.putcol('FLAG', rq_flag)

            # Collect dictionaries per band
            spowerdict[band] = spowerdictband
            dat_common_final[band] = dat_common
            template_table[band] = template_table_band

        # If requested to apply results, copy the now modified temp table over to the original
        if self.inputs.apply:

            LOG.info("Results applied to the RQ table.")
            if os.path.isdir(rq_table):
                # make a table backup from the original, and remove original to be replaced by temprq
                shutil.copytree(rq_table, rq_table + '.backup')
                shutil.rmtree(rq_table)
            shutil.copytree(temprq, rq_table)

            if do_not_apply_spws:

                LOG.warning(
                    "Adopt the original requantizer gains for spws=%r from the band(s) specified by the 'do_not_apply' input parameter.",
                    do_not_apply_spws)
                # copy back the do_not_apply_spws rows from the original table, so they don't get overwritten.
                original_tb = rq_table + '.backup'
                modified_tb = rq_table

                with casa_tools.TableReader(original_tb, nomodify=True) as tb_original:
                    with casa_tools.TableReader(modified_tb, nomodify=False) as tb_modified:

                        rq_par_original = tb_original.getcol('FPARAM')
                        rq_par_modified = tb_modified.getcol('FPARAM')

                        rq_flag_original = tb_original.getcol('FLAG')
                        rq_flag_modified = tb_modified.getcol('FLAG')

                        for spwid in do_not_apply_spws:
                            subtb = tb_original.query('SPECTRAL_WINDOW_ID == '+str(spwid))
                            rows_select = subtb.rownumbers()
                            subtb.close()
                            LOG.debug('copy FPARM/FLAG values from %s to %s for spw= %s', original_tb, modified_tb, spwid)
                            rq_par_modified[:, :, rows_select] = rq_par_original[:, :, rows_select]
                            rq_flag_modified[:, :, rows_select] = rq_flag_original[:, :, rows_select]

                        tb_modified.putcol('FPARAM', rq_par_modified)
                        tb_modified.putcol('FLAG', rq_flag_modified)

        else:
            LOG.info("Results not applied to the RQ table.")

        # Cleanup temporary table that we operated on
        # if os.path.isdir(temprq):
        #     shutil.rmtree(temprq)

        return SyspowerResults(gaintable=rq_table, spowerdict=spowerdict, dat_common=dat_common_final,
                               clip_sp_template=clip_sp_template, template_table=template_table,
                               band_baseband_spw=band_baseband_spw, plotrq=temprq)

    def analyse(self, results):
        return results

    # function for smoothing and statistical flagging
    # adapted from https://gist.github.com/bhawkins/3535131
    def medfilt(self, x, k, threshold=6.0, flag_rms=False, flag_median=False, flag_only=False, fill_gaps=False):

        k2 = (k - 1) // 2
        y = np.ma.zeros((len(x), k))
        y.mask = np.ma.resize(x.mask, (len(x), k))
        y[:, k2] = x

        for i in range(k2):
            j = k2 - i
            y[j:, i] = x[:-j]
            y[:j, i] = x[0]
            y.mask[:j, i] = True
            y[:-j, -(i + 1)] = x[j:]
            y[-j:, -(i + 1)] = x[-1]
            y.mask[-j:, -(i + 1)] = True
        medians = np.ma.median(y, axis=1)

        if isinstance(medians.mask, np.bool_):
            medians.mask = np.ma.getmaskarray(medians)

        if np.ma.all(medians.mask): return medians

        if fill_gaps:
            x[x.mask] = medians[x.mask]
            return x

        if flag_median:
            rms = np.ma.std(y, axis=1)
            dev = np.ma.median(rms[rms != 0])
            medians.mask[abs(x - medians) > (dev * threshold)] = True
            medians.mask[rms == 0] = True
            medians.mask[rms != rms] = True

        if flag_rms:
            rms = np.ma.std(y, axis=1)
            dev = np.ma.median(rms[rms != 0])
            medians.mask[rms > (dev * threshold)] = True
            medians.mask[rms == 0] = True
            medians.mask[rms != rms] = True

        if not flag_only:
            return medians
        else:
            x.mask = np.logical_or(x.mask, medians.mask)
            return x

    # combine SPWs and flag based on moving window statistics
    def flag_with_medfilt(self, x, temp, k=21, threshold=6, do_shift=False, **kwargs):
        if do_shift:
            resid = x.ravel() - np.roll(x.ravel(), -1)
        else:
            resid = (x - temp[np.newaxis, :]).ravel()
        new_flags = self.medfilt(resid, k, threshold=threshold, flag_only=True, **kwargs)
        x.mask = np.reshape(new_flags.mask, newshape=x.shape)
        flag_percent = 100.0 * np.sum(x.mask) / x.size
        x.mask[x == 0] = True
        return x, flag_percent

    # use median filter to interpolate flagged values
    def interp_with_medfilt(self, x, k=21, threshold=99, max_interp=10):
        x.mask = np.ma.getmaskarray(x)
        this_interp = 0
        while np.any(x.mask == True):
            flag_percent = 100.0 * np.sum(x.mask) / x.size
            message = '    will attempt to interpolate {0:.2f}% of data in iteration {1}'.format(flag_percent,
                                                                                                 this_interp + 1)
            if this_interp == 0:
                LOG.info(message)
            else:
                LOG.debug(message)

            x = self.medfilt(x, k, threshold, fill_gaps=True)
            this_interp += 1
            if this_interp > max_interp:
                break
        flag_percent2 = 100.0 * np.sum(x.mask) / x.size
        LOG.info('    finished interpolation with {0:.2f}% of data flagged'.format(flag_percent2))
        x.mask[x == 0] = True
        return x
