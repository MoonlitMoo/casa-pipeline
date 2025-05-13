import math
import os

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


class testBPdcalsSummaryChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        # self.caltable = result.final[0].gaintable

    def plot(self):
        # science_spws = self.ms.get_spectral_windows(science_windows_only=True)
        plots = [self.get_plot_wrapper('BPcal'), self.get_plot_wrapper('delaycal')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        bandpass_field_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_field_select_string
        bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
        corrstring = self.ms.get_vla_corrstring()
        delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string

        if prefix == 'BPcal':
            job = casa_tasks.plotms(vis=self.ms.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected',
                                    selectdata=True,
                                    field=bandpass_field_select_string, scan=bandpass_scan_select_string,
                                    correlation=corrstring, averagedata=True, avgtime='1e8', avgscan=True,
                                    transform=False,
                                    extendflag=False, iteraxis='', coloraxis='antenna2', plotrange=[], title='',
                                    xlabel='', ylabel='', showmajorgrid=False, showminorgrid=False, plotfile=figfile,
                                    overwrite=True, clearplots=True, showgui=False)

        if (delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal':
            job = casa_tasks.plotms(vis=self.ms.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected',
                                    selectdata=True,
                                    scan=delay_scan_select_string, correlation=corrstring, averagedata=True,
                                    avgtime='1e8', avgscan=True, transform=False, extendflag=False, iteraxis='',
                                    coloraxis='antenna2', plotrange=[], title='', xlabel='', ylabel='',
                                    showmajorgrid=False,
                                    showminorgrid=False, plotfile=figfile, overwrite=True, clearplots=True,
                                    showgui=False)

        job.execute()

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'testcalibrated' + prefix + '-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
        delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string

        if prefix == 'BPcal' or ((delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal'):
            wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                                  parameters={'vis': self.ms.basename,
                                              'type': prefix,
                                              'spw': ''})

            if not os.path.exists(figfile):
                LOG.trace('testBPdcals summary plot not found. Creating new '
                          'plot.')
                try:
                    self.create_plot(prefix)
                except Exception as ex:
                    LOG.error('Could not create ' + prefix + ' plot.')
                    LOG.exception(ex)
                    return None

            return wrapper

        return None


class testBPdcalsPerSpwSummaryChart(object):
    def __init__(self, context, result, spw):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.spw = str(spw)

    def plot(self):
        plots = [self.get_plot_wrapper('BPcal'), self.get_plot_wrapper('delaycal')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        bandpass_field_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_field_select_string
        bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
        corrstring = self.ms.get_vla_corrstring()
        delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string

        if prefix == 'BPcal':
            job = casa_tasks.plotms(vis=self.ms.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected',
                                    spw=self.spw, selectdata=True,
                                    field=bandpass_field_select_string, scan=bandpass_scan_select_string,
                                    correlation=corrstring, averagedata=True, avgtime='1e8', avgscan=True,
                                    transform=False,
                                    extendflag=False, iteraxis='', coloraxis='antenna2', plotrange=[], title='',
                                    xlabel='', ylabel='', showmajorgrid=False, showminorgrid=False, plotfile=figfile,
                                    overwrite=True, clearplots=True, showgui=False)

        if (delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal':
            job = casa_tasks.plotms(vis=self.ms.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected',
                                    selectdata=True, spw=self.spw,
                                    scan=delay_scan_select_string, correlation=corrstring, averagedata=True,
                                    avgtime='1e8', avgscan=True, transform=False, extendflag=False, iteraxis='',
                                    coloraxis='antenna2', plotrange=[], title='', xlabel='', ylabel='',
                                    showmajorgrid=False,
                                    showminorgrid=False, plotfile=figfile, overwrite=True, clearplots=True,
                                    showgui=False)

        job.execute()

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'testcalibrated_per_spw_' + self.spw + '_' + prefix + '-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
        delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string

        if prefix == 'BPcal' or ((delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal'):
            wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                                  parameters={'vis': self.ms.basename,
                                              'type': prefix,
                                              'spw': self.spw})

            if not os.path.exists(figfile):
                LOG.trace('testBPdcals summary per-spw plot not found. Creating new '
                          'plot.')
                try:
                    self.create_plot(prefix)
                except Exception as ex:
                    LOG.error('Could not create ' + prefix + ' plot.')
                    LOG.exception(ex)
                    return None

            return wrapper

        return None


class testDelaysPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'testdelays-%s.json' % self.ms)

    def plot(self):

        nplots = len(self.ms.antennas)
        plots = []

        LOG.info("Plotting test delays")

        for bandname, ktypecaltablename in self.result.ktypecaltable.items():

            for ii in range(nplots):

                filename = 'testdelay' + str(ii) + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % self.result.stage_number
                stage_dir = os.path.join(self.context.report_dir, stage)
                # construct the relative filename, eg. 'stageX/testdelay0.png'

                figfile = os.path.join(stage_dir, filename)

                # Get antenna name
                antName = antPlot
                if antPlot != '':
                    domain_antennas = self.ms.get_antenna(antPlot)
                    idents = [a.name if a.name else a.id for a in domain_antennas]
                    antName = ','.join(idents)

                if not os.path.exists(figfile):
                    try:

                        LOG.debug("Plotting test delays {!s}".format(antName))

                        job = casa_tasks.plotms(vis=ktypecaltablename, xaxis='freq', yaxis='amp', field='',
                                                antenna=antPlot, spw='', timerange='', plotrange=[], coloraxis='',
                                                title='K table: {!s}   Antenna: {!s} Band: {!s}'.format(
                                                    ktypecaltablename, antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Frequency', y_axis='Delay', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Test Delay',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class ampGainPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'ampgain-%s.json' % self.ms)

    def plot(self):

        nplots = len(self.ms.antennas)
        plots = []

        times = []
        for bandname, bpdgain_touse in self.result.bpdgain_touse.items():
            with casa_tools.TableReader(bpdgain_touse) as tb:
                times.extend(tb.getcol('TIME'))
        mintime = np.min(times)
        maxtime = np.max(times)

        for bandname, bpdgain_touse in self.result.bpdgain_touse.items():

            with casa_tools.TableReader(bpdgain_touse) as tb:
                cpar = tb.getcol('CPARAM')
                flgs = tb.getcol('FLAG')
            amps = np.abs(cpar)
            good = np.logical_not(flgs)
            maxamp = np.max(amps[good])
            plotmax = maxamp

            LOG.info("Plotting amplitude gain solutions")

            for ii in range(nplots):

                filename = 'testBPdinitialgainamp' + str(ii) + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % self.result.stage_number
                stage_dir = os.path.join(self.context.report_dir, stage)
                # construct the relative filename, eg. 'stageX/testdelay0.png'

                figfile = os.path.join(stage_dir, filename)

                # Get antenna name
                antName = antPlot
                if antPlot != '':
                    domain_antennas = self.ms.get_antenna(antPlot)
                    idents = [a.name if a.name else a.id for a in domain_antennas]
                    antName = ','.join(idents)

                if not os.path.exists(figfile):
                    try:

                        LOG.debug("Plotting amplitude gain solutions " + antName)

                        job = casa_tasks.plotms(vis=bpdgain_touse, xaxis='time', yaxis='amp', field='',
                                                antenna=antPlot, spw='', timerange='',
                                                plotrange=[mintime, maxtime, 0.0, plotmax], coloraxis='',
                                                title='G table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    bpdgain_touse, antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='line')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:

                    plot = logger.Plot(figfile, x_axis='Time', y_axis='Amp', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Amp Gain',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class phaseGainPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'phasegain-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result

        nplots = len(self.ms.antennas)
        plots = []

        times = []
        for bandname, bpdgain_touse in self.result.bpdgain_touse.items():
            with casa_tools.TableReader(bpdgain_touse) as tb:
                times.extend(tb.getcol('TIME'))
        mintime = np.min(times)
        maxtime = np.max(times)

        for bandname, bpdgain_touse in self.result.bpdgain_touse.items():
            with casa_tools.TableReader(bpdgain_touse) as tb:
                cpar = tb.getcol('CPARAM')
                flgs = tb.getcol('FLAG')
            amps = np.abs(cpar)
            good = np.logical_not(flgs)
            maxamp = np.max(amps[good])
            plotmax = maxamp

            LOG.info("Plotting phase gain solutions")

            for ii in range(nplots):

                filename = 'testBPdinitialgainphase' + str(ii) + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % result.stage_number
                stage_dir = os.path.join(context.report_dir, stage)
                # construct the relative filename, eg. 'stageX/testdelay0.png'

                figfile = os.path.join(stage_dir, filename)

                # Get antenna name
                antName = antPlot
                if antPlot != '':
                    domain_antennas = self.ms.get_antenna(antPlot)
                    idents = [a.name if a.name else a.id for a in domain_antennas]
                    antName = ','.join(idents)

                if not os.path.exists(figfile):
                    try:

                        LOG.debug("Plotting phase gain solutions {!s}".format(antName))

                        job = casa_tasks.plotms(vis=bpdgain_touse, xaxis='time', yaxis='phase', field='',
                                                antenna=antPlot, spw='', timerange='',
                                                coloraxis='', plotrange=[mintime, maxtime, -180, 180],
                                                symbolshape='circle',
                                                title='G table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    bpdgain_touse, antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='line')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:

                    plot = logger.Plot(figfile, x_axis='Time', y_axis='Phase', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Phase Gain',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class bpSolAmpPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolamp-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result

        nplots = len(self.ms.antennas)
        plots = []

        for bandname, bpcaltablename in self.result.bpcaltable.items():
            _, maxmaxamp = get_maxphase_maxamp(self.result.bpdgain_touse[bandname], bpcaltablename)
            ampplotmax = maxmaxamp

            LOG.info("Plotting amp bandpass solutions")

            for ii in range(nplots):

                filename = 'testBPcal_amp' + str(ii) + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % result.stage_number
                stage_dir = os.path.join(context.report_dir, stage)
                # construct the relative filename, eg. 'stageX/testdelay0.png'

                figfile = os.path.join(stage_dir, filename)

                # Get antenna name
                antName = antPlot
                if antPlot != '':
                    domain_antennas = self.ms.get_antenna(antPlot)
                    idents = [a.name if a.name else a.id for a in domain_antennas]
                    antName = ','.join(idents)

                if not os.path.exists(figfile):
                    try:

                        LOG.debug("Plotting amp bandpass solutions " + antName)

                        job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='amp', field='',
                                                antenna=antPlot, spw='', timerange='',
                                                coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                                title='B table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    bpcaltablename, antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:

                    plot = logger.Plot(figfile, x_axis='Freq', y_axis='Amp', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Bandpass Amp Solution',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class bpSolAmpPerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolamp-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result

        nplots = len(self.ms.antennas)
        plots = []

        spws = self.ms.get_spectral_windows(science_windows_only=True)
        spw2band = self.ms.get_vla_spw2band()
        for spw in spws:
            if spw.specline_window:
                for bandname, bpcaltablename in self.result.bpcaltable.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    _, maxmaxamp = get_maxphase_maxamp(self.result.bpdgain_touse[bandname], bpcaltablename)
                    ampplotmax = maxmaxamp

                    LOG.info("Plotting amp bandpass solutions")

                    for ii in range(nplots):

                        filename = 'testBPcal_amp' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
                        antPlot = str(ii)

                        stage = 'stage%s' % result.stage_number
                        stage_dir = os.path.join(context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        plot_failed = False

                        # Get antenna name
                        antName = antPlot
                        if antPlot != '':
                            domain_antennas = self.ms.get_antenna(antPlot)
                            idents = [a.name if a.name else a.id for a in domain_antennas]
                            antName = ','.join(idents)

                        if not os.path.exists(figfile):
                            try:

                                LOG.debug("Plotting amp bandpass solutions " + antName)

                                job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='amp', field='',
                                                        antenna=antPlot, timerange='',
                                                        spw=str(spw.id),
                                                        coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                                        title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            bpcaltablename, antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='step')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename)
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                plot = logger.Plot(figfile, x_axis='Freq', y_axis='Amp', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'Bandpass Amp Solution',
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack")
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class bpSolPhasePerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolphase-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result

        nplots = len(self.ms.antennas)
        plots = []

        for bandname, bpcaltablename in self.result.bpcaltable.items():
            maxmaxphase, _ = get_maxphase_maxamp(self.result.bpdgain_touse[bandname], bpcaltablename)
            phaseplotmax = maxmaxphase

            LOG.info("Plotting phase bandpass solutions")

            for ii in range(nplots):

                filename = 'testBPcal_phase' + str(ii) + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % result.stage_number
                stage_dir = os.path.join(context.report_dir, stage)
                # construct the relative filename, eg. 'stageX/testdelay0.png'

                figfile = os.path.join(stage_dir, filename)

                # Get antenna name
                antName = antPlot
                if antPlot != '':
                    domain_antennas = self.ms.get_antenna(antPlot)
                    idents = [a.name if a.name else a.id for a in domain_antennas]
                    antName = ','.join(idents)

                if not os.path.exists(figfile):
                    try:

                        LOG.debug("Plotting phase bandpass solutions " + antName)

                        job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='phase', field='',
                                                antenna=antPlot, spw='', timerange='', coloraxis='',
                                                plotrange=[0, 0, -phaseplotmax, phaseplotmax], symbolshape='circle',
                                                title='B table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    bpcaltablename, antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:

                    plot = logger.Plot(figfile, x_axis='Freq', y_axis='Phase', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Bandpass Phase Solution',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class bpSolPhasePerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolphase-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result

        nplots = len(self.ms.antennas)
        plots = []

        spws = self.ms.get_spectral_windows(science_windows_only=True)
        spw2band = self.ms.get_vla_spw2band()
        for spw in spws:
            if spw.specline_window:
                for bandname, bpcaltablename in self.result.bpcaltable.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    maxmaxphase, _ = get_maxphase_maxamp(self.result.bpdgain_touse[bandname], bpcaltablename)
                    phaseplotmax = maxmaxphase

                    LOG.info("Plotting phase bandpass solutions")

                    for ii in range(nplots):

                        filename = 'testBPcal_phase' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
                        antPlot = str(ii)

                        stage = 'stage%s' % result.stage_number
                        stage_dir = os.path.join(context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        plot_failed = False

                        # Get antenna name
                        antName = antPlot
                        if antPlot != '':
                            domain_antennas = self.ms.get_antenna(antPlot)
                            idents = [a.name if a.name else a.id for a in domain_antennas]
                            antName = ','.join(idents)

                        if not os.path.exists(figfile):
                            try:

                                LOG.debug("Plotting phase bandpass solutions " + antName)

                                job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='phase', field='',
                                                        antenna=antPlot, timerange='', coloraxis='',
                                                        spw=str(spw.id),
                                                        plotrange=[0, 0, -phaseplotmax, phaseplotmax], symbolshape='circle',
                                                        title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            bpcaltablename, antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='step')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename)
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:

                                plot = logger.Plot(figfile, x_axis='Freq', y_axis='Phase', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'Bandpass Phase Solution',
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack")
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


def get_maxphase_maxamp(bpdgain_touse, bpcaltablename):
    """
    Calculates the maximum amplitude and phase values from the bandpass calibration table.

    Args:
        bpdgain_touse (str): The path to the bandpass calibration table.
        bpcaltablename (str): The name of the bandpass calibration table.

    Returns:
        tuple: The maximum amplitude and phase values.
    """
    with casa_tools.TableReader(bpcaltablename) as tb:
        dataVarCol = tb.getvarcol('CPARAM')
        flagVarCol = tb.getvarcol('FLAG')

    rowlist = list(dataVarCol.keys())
    nrows = len(rowlist)
    maxmaxamp = 0.0
    maxmaxphase = 0.0
    for rrow in rowlist:
        dataArr = dataVarCol[rrow]
        flagArr = flagVarCol[rrow]
        amps = np.abs(dataArr)
        phases = np.arctan2(np.imag(dataArr), np.real(dataArr))
        good = np.logical_not(flagArr)
        tmparr = amps[good]
        if len(tmparr) > 0:
            maxamp = np.max(amps[good])
            if maxamp > maxmaxamp:
                maxmaxamp = maxamp
        tmparr = np.abs(phases[good])
        if len(tmparr) > 0:
            maxphase = np.max(np.abs(phases[good])) * 180. / math.pi
            if maxphase > maxmaxphase:
                maxmaxphase = maxphase
    ampplotmax = maxmaxamp
    phaseplotmax = maxmaxphase
    return phaseplotmax, ampplotmax
