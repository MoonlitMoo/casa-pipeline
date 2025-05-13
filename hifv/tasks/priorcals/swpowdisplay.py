import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks

LOG = infrastructure.get_logger(__name__)


class swpowSummaryChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        if result.sw_result.final:
            self.caltable = result.sw_result.final[0].gaintable
        else:
            self.caltable = ''

    def plot(self):
        # science_spws = self.ms.get_spectral_windows(science_windows_only=True)
        plots = [self.get_plot_wrapper('swpow_sample')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        antPlot = '0~2'

        plotmax = 100
        job = casa_tasks.plotms(vis=self.caltable, xaxis='time', yaxis='amp', field='',
                         antenna=antPlot, spw='', timerange='',
                         plotrange=[0, 0, 0, plotmax], coloraxis='',
                         title='Switched Power  swpow.tbl   Antenna: {!s}'.format('0~2'),
                         titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile)

        job.execute()

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'swpow' + prefix + '-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile,
                              x_axis='freq',
                              y_axis='amp',
                              parameters={'vis': self.ms.basename,
                                          'type': prefix,
                                          'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('swpow summary plot not found. Creating new '
                      'plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class swpowPerAntennaChart(object):
    def __init__(self, context, result, yaxis, science_scan_ids, band, selectbasebands, spw):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        ms = self.ms
        self.yaxis = yaxis
        if result.sw_result.final:
            self.caltable = result.sw_result.final[0].gaintable
        else:
            self.caltable = ''
        self.band = band
        self.spw = spw
        self.selectbasebands = selectbasebands

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          yaxis + 'swpow-%s.json' % ms)
        self.science_scan_ids = science_scan_ids

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]
        numAntenna = len(m.antennas)
        plots = []

        LOG.info("Plotting swpowcal charts for " + self.yaxis)
        nplots = numAntenna
        antName = ''
        for ii in range(nplots):

            filename = 'swpow_' + self.yaxis + '_' + str(ii) + '_{!s}'.format(self.band) + '.png'
            antPlot = str(ii)

            stage = 'stage%s' % result.stage_number
            stage_dir = os.path.join(context.report_dir, stage)
            # construct the relative filename, eg. 'stageX/testdelay0.png'

            figfile = os.path.join(stage_dir, filename)

            plotrange = []

            if self.yaxis == 'spgain':
                plotrange = [0, 0, 0, 1.0]
            if self.yaxis == 'tsys':
                plotrange = [0, 0, 0, 100]
                spws = m.get_all_spectral_windows()
                freqs = sorted({spw.max_frequency.value for spw in spws})
                if float(max(freqs)) >= 18000000000.0:
                    plotrange = [0, 0, 0, 200]

            if not os.path.exists(figfile):
                try:
                    # Get antenna name
                    antName = antPlot
                    if antPlot != '':
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)

                    LOG.debug("Switched Power Plot, using antenna={!s} and spw={!s}".format(antName,
                                                                                            self.result.sw_result.spw))
                    numspws = len(self.spw.split(','))
                    pindexlist = list(range(numspws))
                    cplots = [False for i in pindexlist]
                    cplots[0] = True
                    # Extra check for single spw, single baseband SDMs
                    if numspws == 1:
                        pindexlist = [0]
                    for pindex in pindexlist:
                        spwtouse = str(self.spw.split(',')[pindex])
                        baseband = self.selectbasebands[pindex]
                        spwobj = m.get_spectral_windows(task_arg=spwtouse)[0]
                        mean_freq = spwobj.mean_frequency
                        job = casa_tasks.plotms(vis=self.caltable, xaxis='time', yaxis=self.yaxis, field='',
                                                antenna=antPlot, spw=spwtouse, timerange='',
                                                plotindex=pindex, overwrite=True, gridrows=numspws, gridcols=1, rowindex=pindex,
                                                plotrange=plotrange, coloraxis='corr', colindex=0, clearplots=cplots[pindex],
                                                title='Switched Power  swpow.tbl   Antenna: {!s}  {!s}-band {!s} SPW: {!s} Mean Frequency:{!s}'.format(antName, self.band, baseband, spwtouse, mean_freq),
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
                                               'type': self.yaxis,
                                               'band': self.band,
                                               'file': os.path.basename(figfile)})
                plots.append(plot)
            except Exception as ex:
                LOG.warning("Unable to add plot to stack")
                plots.append(None)

        return [p for p in plots if p is not None]
