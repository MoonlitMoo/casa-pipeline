import os
import datetime

import numpy as np
import matplotlib.pyplot as plt
from astropy.time import Time as atime

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.casa_tools as casa_tools

LOG = infrastructure.get_logger(__name__)


class syspowerBoxChart(object):
    def __init__(self, context, result, dat_common, band):
        self.context = context
        self.result = result
        self.dat_common = dat_common
        self.band = band
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = result.gaintable
        # results[4].read()[0].rq_result[0].final[0].gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('syspower_box')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        antenna_names = [a.name for a in self.ms.antennas]

        # box plot of Pdiff template
        dat_common = self.dat_common # self.result.dat_common
        clip_sp_template = self.result.clip_sp_template

        LOG.info("Creating syspower box chart for {!s}-band...".format(self.band))
        plt.clf()
        dshape = dat_common.shape
        ant_dat = np.reshape(dat_common, newshape=(dshape[0], np.product(dshape[1:])))
        ant_dat = np.ma.array(ant_dat)
        ant_dat.mask = np.ma.getmaskarray(ant_dat)
        ant_dat = np.ma.masked_outside(ant_dat, clip_sp_template[0], clip_sp_template[1])
        ant_dat_filtered = [ant_dat[i][~ant_dat.mask[i]] for i in range(dshape[0])]
        plt.boxplot(ant_dat_filtered, whis=10, sym='.')
        plt.xticks(rotation=45)
        plt.ylim(clip_sp_template[0], clip_sp_template[1])
        plt.ylabel('Template Pdiff   {!s}-band'.format(self.band))
        plt.xlabel('Antenna')
        plt.savefig(figfile)

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower-{!s}-'.format(self.band) + prefix + '-%s-box.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename,
                                                                                'type': prefix,
                                                                                'largecaption': 'Template pdiff',
                                                                                'smallcaption': 'Template pdiff',
                                                                                'spw': '',
                                                                                'band': self.band})

        if not os.path.exists(figfile):
            LOG.trace('syspower summary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class syspowerBarChart(object):
    def __init__(self, context, result,  dat_common, band):
        self.context = context
        self.result = result
        self.dat_common = dat_common
        self.band = band
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = result.gaintable
        # results[4].read()[0].rq_result[0].final[0].gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('syspower_bar')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        antenna_names = [a.name for a in self.ms.antennas]

        # box plot of Pdiff template
        dat_common = self.dat_common  # self.result.dat_common
        clip_sp_template = self.result.clip_sp_template

        LOG.info("Creating syspower bar chart for {!s}-band...".format(self.band))
        plt.clf()
        dshape = dat_common.shape
        ant_dat = np.reshape(dat_common, newshape=(dshape[0], np.product(dshape[1:])))
        ant_dat = np.ma.array(ant_dat)
        ant_dat.mask = np.ma.getmaskarray(ant_dat)
        ant_dat = np.ma.masked_outside(ant_dat, clip_sp_template[0], clip_sp_template[1])

        # fraction of flagged data in Pdiff template
        percent_flagged_by_antenna = [100. * np.sum(ant_dat.mask[i]) / ant_dat.mask[i].size for i in range(dshape[0])]
        plt.bar(list(range(dshape[0])), percent_flagged_by_antenna, color='red')
        plt.xticks(rotation=45)
        plt.ylabel('Fraction of Flagged Solutions (%)     {!s}-band'.format(self.band))
        plt.xlabel('Antenna')

        plt.savefig(figfile)

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower-{!s}-'.format(self.band) + prefix + '-%s-bar.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename,
                                                                                'type': prefix,
                                                                                'largecaption': 'Fraction of flagged solutions',
                                                                                'smallcaption': 'Fraction of flagged solutions',
                                                                                'spw': '',
                                                                                'band': self.band})

        if not os.path.exists(figfile):
            LOG.trace('syspower summary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class compressionSummary(object):
    def __init__(self, context, result, spowerdict, band):
        self.context = context
        self.result = result
        self.spowerdict = spowerdict
        self.band = band
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = result.gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('compressionSummary')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)
        LOG.info("Creating syspower compression summary chart for {!s}-band...".format(self.band))

        spws = []
        basebands = []
        for baseband in self.result.band_baseband_spw[self.band]:
            spws.extend(self.result.band_baseband_spw[self.band][baseband])
            basebands.append(baseband)

        with casa_tools.TableReader(self.result.inputs['vis'] + '/SYSPOWER') as tb:
            stb = tb.query('SPECTRAL_WINDOW_ID in [{!s}]'.format(','.join([str(spw) for spw in spws])))
            sp_time = stb.getcol('TIME')

        sorted_time, idx = np.unique(sp_time, return_index=True)

        me = casa_tools.measures
        qa = casa_tools.quanta

        utc_time = []
        for time in sorted_time:
            q1 = qa.quantity(time, 's')
            time1 = qa.time(q1, form='fits')
            try:
                datetime_object = datetime.datetime.strptime(time1[0], '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                timestr = time1[0]
                timestr = timestr.replace('T24', 'T23')
                datetime_object = datetime.datetime.strptime(timestr, '%Y-%m-%dT%H:%M:%S')
                datetime_object += datetime.timedelta(hours=1)
            utc_time.append(datetime_object)

        # Get scans
        m = self.context.observing_run.get_ms(self.result.inputs['vis'])
        scans = m.get_scans(spw=spws)

        scantimes = []

        for scan in scans:
            epoch = scan.start_time
            # PIPE-2156, replacing quanta.splitdate with astropy.time
            # to resolve a bug in plotting
            t = atime(epoch['m0']["value"], format='mjd')
            dd = t.datetime
            # datestring = dd.strftime('%Y-%m-%dT%H:%M:%S')
            scantimes.append({'scanid': scan.id, 'time': dd})

        pdiff = self.spowerdict['spower_common']  # self.result.spowerdict['spower_common']
        pdiff_ma = np.ma.masked_equal(pdiff, 0)

        numsubplots = 4   # Default
        ncorr = 2
        if len(basebands) > 2:
            numsubplots = ncorr * len(basebands)
        fig0, axes = plt.subplots(numsubplots, 1, sharex='col')
        this_alpha = 1.0

        pc = 0  # Running plot count
        for iplot, baseband in enumerate(basebands):
            for jplot, corr in enumerate(['RR', 'LL']):
                axes[pc].plot_date(utc_time, np.ma.max(pdiff_ma, axis=0)[iplot, jplot], 'o', mfc='blue', mew=0,
                                   ms=3, alpha=this_alpha, label='max')
                axes[pc].plot_date(utc_time, np.ma.median(pdiff_ma, axis=0)[iplot, jplot], 'o', mfc='green', mew=0,
                                   ms=3, alpha=this_alpha, label='median')
                axes[pc].plot_date(utc_time, np.ma.min(pdiff_ma, axis=0)[iplot, jplot], 'o', mfc='brown', mew=0,
                                   ms=3, alpha=this_alpha, label='min')
                axes[pc].set_ylim(0.5, 1.2)
                axes[pc].set_ylabel('{!s} {!s}'.format(baseband, corr))
                pc += 1

        # Assumes at least one baseband in the SDM
        for scantime in scantimes:
            axes[0].plot_date([scantime['time']], [1.2], 'k|', markersize=20.0)
            if (scantime['scanid'] % 2) == 0:
                offset = 1.225
            else:
                offset = 1.285
            plottime = scantime['time'] - datetime.timedelta(0, 20)
            axes[0].text(plottime, offset, str(scantime['scanid']), fontsize='xx-small')
        # leg = axes[0].legend(loc='lower center', ncol=3, bbox_to_anchor=(0.5, 1.0), frameon=True, numpoints=1,
        #                      fancybox=False)
        leg = axes[0].legend(loc='center right', ncol=1, bbox_to_anchor=(1.0, 1.52), frameon=True, numpoints=1,
                             fancybox=False)
        # title = axes[0].set_title('P_diff template summary    {!s}-band'.format(self.band))
        title = axes[0].set_title('  ')
        title.set_position([.5, 1.225])

        axes[numsubplots-1].set_xlabel('UTC Day Time [Day HH:MM]      {!s}-band'.format(self.band))

        fig0.set_size_inches(8, 10)

        plt.savefig(figfile)
        leg.set_bbox_to_anchor((0.5, 0.85))
        # mpld3.save_html(fig0, figname.replace('.png', '.html'))
        plt.close(fig0)
        plt.close()

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower-{!s}-'.format(self.band) + prefix + '-%s-compressionSummary.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='time', y_axis='pdiff', parameters={'vis': self.ms.basename,
                                                                                   'type': prefix,
                                                                                   'largecaption': 'Compression pdiff template summary',
                                                                                   'smallcaption': 'Compression summary (scan numbers indicated on the top axis)',
                                                                                   'spw': '',
                                                                                   'band': self.band})

        if not os.path.exists(figfile):
            LOG.trace('syspower compressionSummary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class medianSummary(object):
    def __init__(self, context, result, spowerdict, band):
        self.context = context
        self.result = result
        self.spowerdict = spowerdict
        self.band = band
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = result.gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('medianSummary')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        # Note that the original script needs the following variables (named differently)
        # Original pdiff from spower_common
        # New variable determined via np.ma.masked_equal(pdiff, 0)
        # Second variable determined via np.ma.masked_where(<2nd variable>== 0, <2nd variable>)

        LOG.info("Creating syspower compression median pdiff summary chart for {!s}-band...".format(self.band))

        spws = []
        basebands = []
        for baseband in self.result.band_baseband_spw[self.band]:
            spws.extend(self.result.band_baseband_spw[self.band][baseband])
            basebands.append(baseband)

        with casa_tools.TableReader(self.result.inputs['vis'] + '/SYSPOWER') as tb:
            stb = tb.query('SPECTRAL_WINDOW_ID in [{!s}]'.format(','.join([str(spw) for spw in spws])))
            sp_time = stb.getcol('TIME')

        sorted_time, idx = np.unique(sp_time, return_index=True)

        me = casa_tools.measures
        qa = casa_tools.quanta

        utc_time = []
        for time in sorted_time:
            q1 = qa.quantity(time, 's')
            time1 = qa.time(q1, form='fits')
            try:
                datetime_object = datetime.datetime.strptime(time1[0], '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                timestr = time1[0]
                timestr = timestr.replace('T24', 'T23')
                datetime_object = datetime.datetime.strptime(timestr, '%Y-%m-%dT%H:%M:%S')
                datetime_object += datetime.timedelta(hours=1)
            utc_time.append(datetime_object)

        # Get scans
        m = self.context.observing_run.get_ms(self.result.inputs['vis'])
        scans = m.get_scans(spw=spws)

        scantimes = []

        for scan in scans:
            epoch = scan.start_time
            # PIPE-2156, replacing quanta.splitdate with astropy.time
            # to resolve a bug in plotting
            t = atime(epoch['m0']["value"], format='mjd')
            dd = t.datetime
            # datestring = dd.strftime('%Y-%m-%dT%H:%M:%S')
            scantimes.append({'scanid': scan.id, 'time': dd})

        pd = self.spowerdict['spower_common']  # self.result.spowerdict['spower_common']
        pdiff = np.ma.masked_equal(pd, 0)

        pdiff_ma = np.ma.masked_where(pdiff == 0, pdiff)

        xrange = np.array(range(pdiff.shape[3]))
        pdiff_ma.data[pdiff_ma > 1.1] = 1.1
        pdiff_ma.data[pdiff_ma < 0.8] = 0.7
        n_blank = 100

        # https://github.com/numpy/numpy/issues/14650

        try:
            pdiff_ma.mask[:, :, :, :n_blank] = True
        except Exception as e:
            if type(pdiff_ma.mask) == np.bool_:
                pdiff_ma.mask = np.ma.getmaskarray(pdiff_ma)
            pdiff_ma.mask[:, :, :, :n_blank] = True
            LOG.debug("Issue with the array mask {!s}".format(e))
            LOG.info("No zero values in pdiff - mask value set to a scalar boolean.  Reset to a matrix. ")

        ma_medians = np.ma.median(pdiff_ma, axis=0)

        fig0 = plt.figure()

        for iplot, baseband in enumerate(basebands):
            for jplot, corr in enumerate(['RR', 'LL']):
                these_medians = ma_medians[iplot, jplot, :]
                hits = np.logical_not(these_medians.mask)
                plt.plot_date(np.array(utc_time)[hits], these_medians[hits], 'o', mew=0, ms=5, alpha=1.0,
                              label='{!s} {!s}'.format(baseband, corr))

        # Scale
        plt.ylim(0.98 * np.min(ma_medians), 1.01 * np.max(ma_medians))

        for scantime in scantimes:
            plt.plot_date([scantime['time']], [1.01 * np.max(ma_medians)], 'k|', markersize=20.0)
            if (scantime['scanid'] % 2) == 0:
                offset = 1.0105
            else:
                offset = 1.0120
            plottime = scantime['time'] - datetime.timedelta(0, 20)
            plt.text(plottime, offset * np.max(ma_medians), str(scantime['scanid']), fontsize='xx-small')

        # plt.xlim(0, pdiff.shape[3])
        leg = plt.legend(loc='upper center', ncol=6, bbox_to_anchor=(0.5, 1.15), fontsize='x-small')
        plt.xlabel('UTC Day Time [Day HH:MM]')
        plt.ylabel('median P_diff     {!s}-band'.format(self.band))
        # p lt.ticklabel_format(useOffset=False)
        plt.gcf().set_size_inches(8, 7)
        plt.savefig(figfile)
        leg.set_bbox_to_anchor((0.5, 0.99))
        # mpld3.save_html(fig0, figname.replace('.png', '.html'))
        plt.close(fig0)
        plt.close()

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower-{!s}-'.format(self.band) + prefix + '-%s-medianSummary.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='time', y_axis='pdiff', parameters={'vis': self.ms.basename,
                                                                                  'type': prefix,
                                                                                  'largecaption': 'Median pdiff summary',
                                                                                  'smallcaption': 'Median pdiff summary (scan numbers indicated on the top axis)',
                                                                                  'spw': '',
                                                                                  'band': self.band})

        if not os.path.exists(figfile):
            LOG.trace('syspower medianSummary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class syspowerPerAntennaChart(object):
    def __init__(self, context, result, yaxis, caltable, fileprefix, tabletype, band, spw, selectbasebands, science_scan_ids):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.yaxis = yaxis
        self.caltable = caltable
        self.fileprefix = fileprefix
        self.tabletype = tabletype
        self.band = band
        self.spw = spw
        self.selectbasebands = selectbasebands
        self.science_scan_ids = science_scan_ids

        self.json = {}
        self.json_filename = os.path.join(context.report_dir, 'stage%s' % result.stage_number,
                                          yaxis + fileprefix + tabletype + '-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]
        numAntenna = len(m.antennas)
        plots = []

        LOG.info("Plotting syspower " + self.tabletype + " charts for " + self.yaxis +
                 " and {!s}-band".format(self.band))
        nplots = numAntenna

        for ii in range(nplots):

            filename = self.fileprefix + '_' + self.tabletype + '_' + '{!s}_'.format(self.band) + self.yaxis + str(ii) + '.png'
            antPlot = str(ii)

            stage = 'stage%s' % result.stage_number
            stage_dir = os.path.join(context.report_dir, stage)

            figfile = os.path.join(stage_dir, filename)

            plotrange = []

            if self.yaxis == 'spgain':
                plotrange = [0, 0, 0, 0.1]
            if self.yaxis == 'tsys':
                plotrange = [0, 0, 0, 100]
                spws = m.get_all_spectral_windows()
                freqs = sorted({spw.max_frequency.value for spw in spws})
                if float(max(freqs)) >= 18000000000.0:
                    plotrange = [0, 0, 0, 200]
            if self.tabletype == 'pdiff':
                clip_sp_template = self.result.clip_sp_template
                plotrange = [-1, -1, clip_sp_template[0], clip_sp_template[1]]

            if not os.path.exists(figfile):
                try:
                    # Get antenna name
                    antName = antPlot
                    if antPlot != '':
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)

                    LOG.debug("Sys Power Plot, using antenna={!s}".format(antName))

                    tabletype = self.tabletype
                    if self.tabletype == 'pdiff':
                        tabletype = 'pdfif_{!s}'.format(self.band)

                    numspws = len(self.spw.split(','))
                    pindexlist = list(range(numspws))
                    cplots = [False for i in pindexlist]
                    cplots[0] = True

                    # Extra check for single spw, single baseband SDMs
                    if numspws == 1:
                        pindexlist = [0]

                    for pindex in pindexlist:

                        spwtouse = self.spw.split(',')[pindex]
                        baseband = self.selectbasebands[pindex]
                        spwobj = m.get_spectral_windows(task_arg=spwtouse)[0]
                        mean_freq = spwobj.mean_frequency

                        job = casa_tasks.plotms(vis=self.caltable, xaxis='time', yaxis=self.yaxis, field='',
                                                antenna=antPlot, spw=spwtouse, timerange='',
                                                plotindex=pindex, gridrows=numspws, gridcols=1, rowindex=pindex,
                                                colindex=0, plotrange=plotrange, coloraxis='corr', overwrite=True,
                                                clearplots=cplots[pindex],
                                                title='Sys Power ' + tabletype +
                                                      '.tbl  Antenna: {!s}  {!s}-band  {!s}  spw: {!s}   {!s}'.format(antName,
                                                                                                         self.band,
                                                                                                         baseband,
                                                                                                         spwtouse,
                                                                                                         mean_freq),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile, scan=self.science_scan_ids)

                        job.execute()

                except Exception as ex:
                    LOG.warning("Unable to plot " + filename)
            else:
                LOG.debug('Using existing ' + filename + ' plot.')

            try:
                plot = logger.Plot(figfile, x_axis='Time', y_axis=self.yaxis.title(), field='',
                                   parameters={'spw': self.spw,
                                               'pol': '',
                                               'ant': antName,
                                               'type': self.tabletype,
                                               'band': self.band,
                                               'file': os.path.basename(figfile)})
                plots.append(plot)
            except Exception as ex:
                LOG.warning("Unable to add plot to stack")
                plots.append(None)

        return [p for p in plots if p is not None]
