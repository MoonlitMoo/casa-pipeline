import math
import os

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


class semifinalBPdcalsSummaryChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        # self.caltable = result.final[0].gaintable

    def plot(self):
        # science_spws = self.ms.get_spectral_windows(science_windows_only=True)
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        context = self.context
        m = context.observing_run.measurement_sets[0]

        corrstring = m.get_vla_corrstring()
        calibrator_scan_select_string = context.evla['msinfo'][m.name].calibrator_scan_select_string

        job = casa_tasks.plotms(vis=m.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected', selectdata=True,
                         scan=calibrator_scan_select_string, correlation=corrstring, averagedata=True, avgtime='1e8',
                         avgscan=False, transform=False, extendflag=False, iteraxis='', coloraxis='antenna2',
                         plotrange=[], title='', xlabel='', ylabel='', showmajorgrid=False, showminorgrid=False,
                         plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

        job.execute()

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'semifinalcalibrated_' + self.suffix + '-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()
        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                              parameters={'vis': self.ms.basename,
                                          'type': 'semifinalcalibratedcals' + self.suffix,
                                          'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('semifinalBPdcals summary plot not found. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create plot.')
                LOG.exception(ex)
                return None
        return wrapper


class semifinalBPdcalsSpwSummaryChart(object):
    def __init__(self, context, result, suffix='', spw=None):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        self.spw = str(spw)

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        context = self.context
        m = context.observing_run.measurement_sets[0]

        corrstring = m.get_vla_corrstring()
        calibrator_scan_select_string = context.evla['msinfo'][m.name].calibrator_scan_select_string

        job = casa_tasks.plotms(vis=m.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected', selectdata=True,
                         scan=calibrator_scan_select_string, correlation=corrstring, averagedata=True, avgtime='1e8',
                         spw=self.spw, avgscan=False, transform=False, extendflag=False, iteraxis='', coloraxis='antenna2',
                         plotrange=[], title='', xlabel='', ylabel='', showmajorgrid=False, showminorgrid=False,
                         plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

        job.execute()

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'semifinalcalibrated_per_spw_' + self.spw + '_' + self.suffix + '-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()
        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                              parameters={'vis': self.ms.basename,
                                          'type': 'semifinalcalibratedcals per spw' + self.suffix,
                                          'spw': self.spw})

        if not os.path.exists(figfile):
            LOG.trace('semifinalBPdcals per-spw summary plot not found. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create plot.')
                LOG.exception(ex)
                return None
        return wrapper


class DelaysPerAntennaChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'delays-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting semiFinal delays")

        for bandname, ktypecaltablename in self.result.ktypecaltable.items():
            for ii in range(nplots):
                filename = 'delay' + str(ii) + '_' + self.suffix + '_' + bandname + '.png'
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

                        LOG.debug("Plotting semiFinal delays " + antName)

                        job = casa_tasks.plotms(vis=ktypecaltablename, xaxis='freq', yaxis='amp', field='',
                                                antenna=antPlot, spw='', timerange='',
                                                plotrange=[], coloraxis='',
                                                title='K table: delay.tbl   Antenna: {!s}  Band: {!s}'.format(
                                                    antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Frequency', y_axis='Delay', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'delay' + self.suffix,
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class semifinalphaseGainPerAntennaChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'phasegain-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        times = []
        for bandname, bpdgain_touse in self.result.bpdgain_touse.items():
            with casa_tools.TableReader(bpdgain_touse) as tb:
                times.extend(tb.getcol('TIME'))
        mintime = np.min(times)
        maxtime = np.max(times)

        LOG.info("Plotting phase gain solutions")

        for bandname, bpdgain_touse in self.result.bpdgain_touse.items():

            for ii in range(nplots):
                filename = 'BPinitialgainphase' + str(ii) + '_' + self.suffix + '_' + bandname + '.png'
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

                        LOG.debug("Plotting phase gain solutions " + antName)

                        job = casa_tasks.plotms(vis=bpdgain_touse, xaxis='time', yaxis='phase', field='',
                                                antenna=antPlot, spw='', timerange='',
                                                coloraxis='', plotrange=[mintime, maxtime, -180, 180], symbolshape='circle',
                                                title='G table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    bpdgain_touse, antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='line')

                        job.execute()

                    except:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:

                    plot = logger.Plot(figfile, x_axis='Time', y_axis='Phase', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'phasegain' + self.suffix,
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class semifinalbpSolAmpPerAntennaChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolamp-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        LOG.info("Plotting amp bandpass solutions")

        for bandname, bpcaltablename in self.result.bpcaltable.items():

            with casa_tools.TableReader(self.result.bpcaltable[bandname]) as tb:
                dataVarCol = tb.getvarcol('CPARAM')
                flagVarCol = tb.getvarcol('FLAG')

            rowlist = list(dataVarCol.keys())
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

            for ii in range(nplots):
                filename = 'BPcal_amp' + str(ii) + '_' + self.suffix + '_' + bandname + '.png'
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

                        job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='amp', field='',
                                                antenna=antPlot, spw='', timerange='',
                                                coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                                title='B table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    'BPcal.b', antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Freq', y_axis='Amp', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'bpsolamp' + self.suffix,
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class semifinalbpSolAmpPerAntennaPerSpwChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolamp-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]
        spws = m.get_spectral_windows(science_windows_only=True)
        spw2band = self.ms.get_vla_spw2band()
        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting amp bandpass solutions per spw for spectral window spws")

        for spw in spws:
            if spw.specline_window:
                for bandname, bpcaltablename in self.result.bpcaltable.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    with casa_tools.TableReader(self.result.bpcaltable[bandname]) as tb:
                        dataVarCol = tb.getvarcol('CPARAM')
                        flagVarCol = tb.getvarcol('FLAG')

                    rowlist = list(dataVarCol.keys())
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

                    for ii in range(nplots):
                        filename = 'BPcal_amp' + str(ii) + '_' + self.suffix + '_' + bandname + '_' + str(spw.id) + '.png'
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

                                job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='amp', field='',
                                                        antenna=antPlot, timerange='',
                                                        spw=str(spw.id),
                                                        coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                                        title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            'BPcal.b', antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='step')

                                job.execute()

                            except:
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
                                                            'type': 'bpsolamp' + self.suffix,
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except:
                                LOG.warning("Unable to add plot to stack")
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class semifinalbpSolPhasePerAntennaChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolphase-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        LOG.info("Plotting phase bandpass solutions")

        for bandname, bpcaltablename in self.result.bpcaltable.items():

            with casa_tools.TableReader(self.result.bpcaltable[bandname]) as tb:
                dataVarCol = tb.getvarcol('CPARAM')
                flagVarCol = tb.getvarcol('FLAG')

            rowlist = list(dataVarCol.keys())
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
            phaseplotmax = maxmaxphase

            for ii in range(nplots):
                filename = 'BPcal_phase' + str(ii) + '_' + self.suffix + '_' + bandname + '.png'
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

                        job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='phase', field='',
                                                antenna=antPlot, spw='', timerange='',
                                                coloraxis='', plotrange=[0, 0, -phaseplotmax, phaseplotmax],
                                                symbolshape='circle',
                                                title='B table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    'BPcal.tbl', antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Freq', y_axis='Phase', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'bpsolphase' + self.suffix,
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

            # Get BPcal.b to close...

        return [p for p in plots if p is not None]


class semifinalbpSolPhasePerAntennaPerSpwChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolphase-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        LOG.info("Plotting phase bandpass solutions for spectral windows spws")
        spws = m.get_spectral_windows(science_windows_only=True)
        spw2band = self.ms.get_vla_spw2band()
        for spw in spws:
            if spw.specline_window:
                for bandname, bpcaltablename in self.result.bpcaltable.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    with casa_tools.TableReader(self.result.bpcaltable[bandname]) as tb:
                        dataVarCol = tb.getvarcol('CPARAM')
                        flagVarCol = tb.getvarcol('FLAG')

                    rowlist = list(dataVarCol.keys())
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
                    phaseplotmax = maxmaxphase

                    for ii in range(nplots):
                        filename = 'BPcal_phase' + str(ii) + '_' + self.suffix + '_' + bandname + '_' + str(spw.id) + '.png'
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

                                job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='phase', field='',
                                                        antenna=antPlot, timerange='',
                                                        spw=str(spw.id),
                                                        coloraxis='', plotrange=[0, 0, -phaseplotmax, phaseplotmax],
                                                        symbolshape='circle',
                                                        title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            'BPcal.tbl', antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='step')

                                job.execute()

                            except:
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
                                                            'type': 'bpsolphase' + self.suffix,
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except:
                                LOG.warning("Unable to add plot to stack")
                                plots.append(None)
                        else:
                            plots.append(None)

                # Get BPcal.b to close...

        return [p for p in plots if p is not None]
