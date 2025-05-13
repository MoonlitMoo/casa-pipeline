import math
import os
import collections

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


class finalcalsSummaryChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])
        # self.caltable = result.final[0].gaintable

    def plot(self):
        # science_spws = self.ms.get_spectral_windows(science_windows_only=True)
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        job = casa_tasks.plotms(vis=self.result.ktypecaltable, xaxis='freq', yaxis='amp', field='',
                         antenna='0~2', spw='', timerange='',
                         plotrange=[], coloraxis='spw',
                         title='K table: finaldelay.tbl   Antenna: {!s}'.format('0~2'),
                         titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile)

        job.execute()

    def get_figfile(self):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'finalcalsjunk' + '-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='delay',
                              parameters={'vis': self.ms.basename,
                                          'type': 'finalcalsjunk',
                                          'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('finalcals summary plot not found. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create finalcals plot.')
                LOG.exception(ex)
                return None

        return wrapper


class finalDelaysPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir, 'stage%s' % result.stage_number,
                                          'finaldelays-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]
        plots = []
        nplots = len(m.antennas)

        LOG.info("Plotting finalDelay calibration tables")

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        for bandname, spwlist in band2spw.items():
            for ii in range(nplots):

                filename = 'finaldelay' + str(ii) + '_' + bandname + '.png'
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

                        LOG.debug("Plotting final calibration tables " + antName)

                        job = casa_tasks.plotms(vis=self.result.ktypecaltable, xaxis='freq', yaxis='amp', field='',
                                                antenna=antPlot, spw=','.join(spwlist), timerange='',
                                                plotrange=[], coloraxis='',
                                                title='K table: finaldelay.tbl   Antenna: {!s}  Band: {!s}'.format(
                                                    antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename + str(ex))
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    real_figfile = figfile

                    plot = logger.Plot(real_figfile, x_axis='Frequency', y_axis='Delay', field='',
                                       parameters={'spw': ','.join(spwlist),
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Final delay',
                                                   'file': os.path.basename(real_figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack." + str(ex))
                    plots.append(None)

        return [p for p in plots if p is not None]


class finalphaseGainPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir, 'stage%s' % result.stage_number,
                                          'finalphasegain-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting final phase gain solutions")

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        with casa_tools.TableReader(result.bpdgain_touse) as tb:
            times = tb.getcol('TIME')
        mintime = np.min(times)
        maxtime = np.max(times)

        for bandname, spwlist in band2spw.items():
            for ii in range(nplots):

                filename = 'finalBPinitialgainphase' + str(ii) + '_' + bandname + '.png'
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

                        LOG.debug("Plotting final phase gain solutions " + antName)
                        job = casa_tasks.plotms(vis=result.bpdgain_touse, xaxis='time', yaxis='phase', field='',
                                                antenna=antPlot, spw=','.join(spwlist), timerange='',
                                                coloraxis='', plotrange=[mintime, maxtime, -180, 180], symbolshape='circle',
                                                title='G table: finalBPinitialgain.tbl   Antenna: {!s}  Band: {!s}'.format(
                                                    antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='line')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename + str(ex))
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Time', y_axis='Phase', field='',
                                       parameters={'spw': ','.join(spwlist),
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'BP initial gain phase',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack.  " + str(ex))
                    plots.append(None)

        return [p for p in plots if p is not None]


class finalbpSolAmpPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalbpsolamp-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        for bandname, spwlist in band2spw.items():
            maxmaxphase, maxmaxamp = get_maxphase_maxamp(self.result.bpcaltable)
            ampplotmax = maxmaxamp

            LOG.info("Plotting amp bandpass solutions")

            for ii in range(nplots):

                filename = 'finalBPcal_amp' + str(ii) + '_' + bandname + '.png'
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
                        job = casa_tasks.plotms(vis=self.result.bpcaltable, xaxis='freq', yaxis='amp', field='',
                                                antenna=antPlot, spw=','.join(spwlist), timerange='',
                                                coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                                title='B table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    'finalBPcal.tbl', antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename + str(ex))
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    real_figfile = figfile

                    plot = logger.Plot(real_figfile, x_axis='Freq', y_axis='Amp', field='',
                                       parameters={'spw': ','.join(spwlist),
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'BP Amp solution',
                                                   'file': os.path.basename(real_figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack.  " + str(ex))
                    plots.append(None)

        return [p for p in plots if p is not None]


class finalbpSolAmpPerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalbpsolamp-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        for spw in spwobjlist:
            if spw.specline_window:
                for bandname, spwlist in band2spw.items():
                    if spw2band[spw.id] != bandname:
                        continue                    
                    maxmaxphase, maxmaxamp = get_maxphase_maxamp(self.result.bpcaltable)
                    ampplotmax = maxmaxamp

                    LOG.info("Plotting amp bandpass solutions")

                    for ii in range(nplots):

                        filename = 'finalBPcal_amp' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
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
                                job = casa_tasks.plotms(vis=self.result.bpcaltable, xaxis='freq', yaxis='amp', field='',
                                                        antenna=antPlot, spw=str(spw.id), timerange='',
                                                        coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                                        title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            'finalBPcal.tbl', antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='step')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename + str(ex))
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                real_figfile = figfile

                                plot = logger.Plot(real_figfile, x_axis='Freq', y_axis='Amp', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'BP Amp solution',
                                                            'file': os.path.basename(real_figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack.  " + str(ex))
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class finalbpSolPhasePerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalbpsolphase-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        for bandname, spwlist in band2spw.items():
            maxmaxphase, maxmaxamp = get_maxphase_maxamp(self.result.bpcaltable)
            phaseplotmax = maxmaxphase

            LOG.info("Plotting phase bandpass solutions")

            for ii in range(nplots):

                filename = 'finalBPcal_phase' + str(ii) + '_' + bandname + '.png'
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
                        job = casa_tasks.plotms(vis=self.result.bpcaltable, xaxis='freq', yaxis='phase', field='',
                                                antenna=antPlot, spw=','.join(spwlist), timerange='',
                                                coloraxis='', plotrange=[0, 0, -phaseplotmax, phaseplotmax],
                                                symbolshape='circle',
                                                title='B table: {!s}   Antenna: {!s}  Band: {!s}'.format(
                                                    'finalBPcal.tbl', antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename + str(ex))
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    real_figfile = figfile

                    plot = logger.Plot(real_figfile, x_axis='Freq', y_axis='Phase', field='',
                                       parameters={'spw': ','.join(spwlist),
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'BP Phase solution',
                                                   'file': os.path.basename(real_figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack.  " + str(ex))
                    plots.append(None)

        return [p for p in plots if p is not None]


class finalbpSolPhasePerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalbpsolphase-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        for spw in spwobjlist:
            if spw.specline_window:
                for bandname, spwlist in band2spw.items():
                    if spw2band[spw.id] != bandname:
                        continue                    
                    maxmaxphase, maxmaxamp = get_maxphase_maxamp(self.result.bpcaltable)
                    phaseplotmax = maxmaxphase

                    LOG.info("Plotting phase bandpass solutions")

                    for ii in range(nplots):

                        filename = 'finalBPcal_phase' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
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
                                job = casa_tasks.plotms(vis=self.result.bpcaltable, xaxis='freq', yaxis='phase', field='',
                                                        antenna=antPlot, spw=str(spw.id), timerange='',
                                                        coloraxis='', plotrange=[0, 0, -phaseplotmax, phaseplotmax],
                                                        symbolshape='circle',
                                                        title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            'finalBPcal.tbl', antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='step')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename + str(ex))
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                real_figfile = figfile

                                plot = logger.Plot(real_figfile, x_axis='Freq', y_axis='Phase', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'BP Phase solution',
                                                            'file': os.path.basename(real_figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack.  " + str(ex))
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class finalbpSolPhaseShortPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalbpsolphaseshort-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting phase short gaincal")

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        with casa_tools.TableReader(self.result.phaseshortgaincaltable) as tb:
            times = tb.getcol('TIME')
        mintime = np.min(times)
        maxtime = np.max(times)

        for bandname, spwlist in band2spw.items():

            for ii in range(nplots):

                filename = 'phaseshortgaincal' + str(ii) + '_' + bandname + '.png'
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

                        LOG.debug("Plotting phase short gaincal " + antName)
                        job = casa_tasks.plotms(vis=self.result.phaseshortgaincaltable, xaxis='time', yaxis='phase', field='',
                                                antenna=antPlot, spw=','.join(spwlist), timerange='',
                                                coloraxis='', plotrange=[mintime, maxtime, -180, 180], symbolshape='circle',
                                                title='G table: phaseshortgaincal.tbl   Antenna: {!s}  Band: {!s}'.format(
                                                    antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='line')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename + str(ex))
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Time', y_axis='Phase', field='',
                                       parameters={'spw': ','.join(spwlist),
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Phase (short) gain solution',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack.  " + str(ex))
                    plots.append(None)

        return [p for p in plots if p is not None]


class finalAmpTimeCalPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalamptimecal-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        with casa_tools.TableReader(self.result.finalampgaincaltable) as tb:
            times = tb.getcol('TIME')
        mintime = np.min(times)
        maxtime = np.max(times)

        for bandname, spwlist in band2spw.items():

            with casa_tools.TableReader(self.result.finalampgaincaltable) as tb:
                cpar = tb.getcol('CPARAM')
                flgs = tb.getcol('FLAG')
            amps = np.abs(cpar)
            good = np.logical_not(flgs)
            maxamp = np.max(amps[good])
            plotmax = max(2.0, maxamp)

            LOG.info("Plotting final amp timecal")

            for ii in range(nplots):

                filename = 'finalamptimecal' + str(ii) + '_' + bandname + '.png'
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

                        LOG.debug("Plotting final amp timecal " + antName)
                        job = casa_tasks.plotms(vis=self.result.finalampgaincaltable, xaxis='time', yaxis='amp', field='',
                                                antenna=antPlot, spw=','.join(spwlist), timerange='',
                                                coloraxis='', plotrange=[mintime, maxtime, 0, plotmax], symbolshape='circle',
                                                title='G table: finalampgaincal.tbl   Antenna: {!s}  Band: {!s}'.format(
                                                    antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='line')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename + str(ex))
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Time', y_axis='Amp', field='',
                                       parameters={'spw': ','.join(spwlist),
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Final amp time cal',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack.  " + str(ex))
                    plots.append(None)

        return [p for p in plots if p is not None]


class finalAmpTimeCalPerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalamptimecal-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        with casa_tools.TableReader(self.result.finalampgaincaltable) as tb:
            times = tb.getcol('TIME')
        mintime = np.min(times)
        maxtime = np.max(times)

        for spw in spwobjlist:
            if spw.specline_window:
                for bandname, spwlist in band2spw.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    with casa_tools.TableReader(self.result.finalampgaincaltable) as tb:
                        cpar = tb.getcol('CPARAM')
                        flgs = tb.getcol('FLAG')
                    amps = np.abs(cpar)
                    good = np.logical_not(flgs)
                    maxamp = np.max(amps[good])
                    plotmax = max(2.0, maxamp)

                    LOG.info("Plotting final amp timecal")

                    for ii in range(nplots):

                        filename = 'finalamptimecal' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
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

                                LOG.debug("Plotting final amp timecal " + antName)
                                job = casa_tasks.plotms(vis=self.result.finalampgaincaltable, xaxis='time', yaxis='amp', field='',
                                                        antenna=antPlot, spw=str(spw.id), timerange='',
                                                        coloraxis='', plotrange=[mintime, maxtime, 0, plotmax], symbolshape='circle',
                                                        title='G table: finalampgaincal.tbl   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='line')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename + str(ex))
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                plot = logger.Plot(figfile, x_axis='Time', y_axis='Amp', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'Final amp time cal',
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack.  " + str(ex))
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class finalAmpFreqCalPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalampfreqcal-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        for bandname, spwlist in band2spw.items():

            with casa_tools.TableReader(self.result.finalampgaincaltable) as tb:
                cpar = tb.getcol('CPARAM')
                flgs = tb.getcol('FLAG')
            amps = np.abs(cpar)
            good = np.logical_not(flgs)
            maxamp = np.max(amps[good])
            plotmax = max(2.0, maxamp)

            LOG.info("Plotting final amp freqcal")

            for ii in range(nplots):
                filename = 'finalampfreqcal' + str(ii) + '_' + bandname + '.png'
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

                        LOG.debug("Plotting final amp freqcal " + antName)
                        job = casa_tasks.plotms(vis=self.result.finalampgaincaltable, xaxis='freq', yaxis='amp', field='',
                                                antenna=antPlot, spw=','.join(spwlist), timerange='',
                                                coloraxis='', plotrange=[0, 0, 0, plotmax], symbolshape='circle',
                                                title='G table: finalampgaincal.tbl   Antenna: {!s}  Band: {!s}'.format(
                                                    antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename + str(ex))
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    real_figfile = figfile

                    plot = logger.Plot(real_figfile, x_axis='freq', y_axis='Amp', field='',
                                       parameters={'spw': ','.join(spwlist),
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Final amp freq cal',
                                                   'file': os.path.basename(real_figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack.  " + str(ex))
                    plots.append(None)

        return [p for p in plots if p is not None]


class finalPhaseGainCalPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalphasegaincal-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting final phase freqcal")

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        with casa_tools.TableReader(self.result.finalphasegaincaltable) as tb:
            times = tb.getcol('TIME')
        mintime = np.min(times)
        maxtime = np.max(times)

        for bandname, spwlist in band2spw.items():

            for ii in range(nplots):

                filename = 'finalphasegaincal' + str(ii) + '_' + bandname + '.png'
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

                        LOG.debug("Plotting final phase freqcal " + antName)
                        job = casa_tasks.plotms(vis=self.result.finalphasegaincaltable, xaxis='time', yaxis='phase', field='',
                                                antenna=antPlot, spw=','.join(spwlist), timerange='',
                                                coloraxis='', plotrange=[mintime, maxtime, -180, 180], symbolshape='circle',
                                                title='G table: finalphasegaincal.tbl   Antenna: {!s}  Band: {!s}'.format(
                                                    antName, bandname),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='line')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Problem with plotting " + filename + str(ex))
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='time', y_axis='phase', field='',
                                       parameters={'spw': ','.join(spwlist),
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'Final phase gain cal',
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack.  " + str(ex))
                    plots.append(None)

        return [p for p in plots if p is not None]


class finalPhaseGainCalPerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalphasegaincal-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        spws = m.get_spectral_windows(science_windows_only=True)
        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting final phase freqcal per spw for spectral window spws")

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        with casa_tools.TableReader(self.result.finalphasegaincaltable) as tb:
            times = tb.getcol('TIME')
        mintime = np.min(times)
        maxtime = np.max(times)

        for spw in spwobjlist:
            if spw.specline_window:
                for bandname, spwlist in band2spw.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    for ii in range(nplots):

                        filename = 'finalphasegaincal' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
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

                                LOG.debug("Plotting final phase freqcal " + antName)
                                job = casa_tasks.plotms(vis=self.result.finalphasegaincaltable, xaxis='time', yaxis='phase', field='',
                                                        antenna=antPlot, spw=str(spw.id), timerange='',
                                                        coloraxis='', plotrange=[mintime, maxtime, -180, 180], symbolshape='circle',
                                                        title='G table: finalphasegaincal.tbl   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='line')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Problem with plotting " + filename + str(ex))
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                plot = logger.Plot(figfile, x_axis='time', y_axis='phase', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'Final phase gain cal',
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack.  " + str(ex))
                                plots.append(None)
                        else: 
                            plots.append(None)

        return [p for p in plots if p is not None]


def get_maxphase_maxamp(bpcaltable):
    """
    Get the max amplitude and max phase for a given bpcaltable
    """
    with casa_tools.TableReader(bpcaltable) as tb:
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
    return maxmaxphase, maxmaxamp
