import os

import numpy as np
import matplotlib.pyplot as plt

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks

LOG = infrastructure.get_logger(__name__)


class fluxbootSummaryChart(object):
    """
    Handles the creation of the "Model calibrator. Plot of amp vs. freq." figure in plotms
    """

    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        context = self.context
        m = context.observing_run.measurement_sets[0]
        corrstring = m.get_vla_corrstring()
        calibrator_scan_select_string = context.evla['msinfo'][m.name].calibrator_scan_select_string
        ms_active = m.name

        job = casa_tasks.plotms(vis=ms_active, xaxis='freq', yaxis='amp', ydatacolumn='model', selectdata=True,
                         scan=calibrator_scan_select_string, correlation=corrstring, averagedata=True,
                         avgtime='1e8', avgscan=True, transform=False,    extendflag=False, iteraxis='',
                         coloraxis='field', plotrange=[], title='', xlabel='', ylabel='',  showmajorgrid=False,
                         showminorgrid=False, plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

        job.execute()

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'bootstrappedFluxDensities-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                              parameters={'vis': self.ms.basename,
                                          'type': 'fluxboot',
                                          'spw': '',
                                          'figurecaption':'Model calibrator.  Plot of amp vs. freq.'})

        if not os.path.exists(figfile):
            LOG.trace('Plotting model calibrator flux densities. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create fluxboot plot.')
                LOG.exception(ex)
                return None

        return wrapper


class fluxgaincalSummaryChart(object):
    """
    Handles the creation of the "Caltable: fluxgaincal.g. Plot of amp vs. freq." figure in plotms
    """

    def __init__(self, context, result, caltable):
        self.context = context
        self.result = result
        self.caltable = caltable
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        job = casa_tasks.plotms(vis=self.caltable, xaxis='freq', yaxis='amp', ydatacolumn='', selectdata=True,
                         scan='', correlation='', averagedata=True,
                         avgtime='', avgscan=False, transform=False, extendflag=False, iteraxis='',
                         coloraxis='field', plotrange=[], title='', xlabel='', ylabel='', showmajorgrid=False,
                         showminorgrid=False, plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

        job.execute()

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'fluxgaincalFluxDensities-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                              parameters={'vis': self.ms.basename,
                                          'type': 'fluxgaincal',
                                          'spw': '',
                                          'caltable': self.caltable,
                                          'figurecaption': 'Caltable: {!s}. Plot of amp vs. freq.'.format(self.caltable)})

        if not os.path.exists(figfile):
            LOG.trace('Plotting amp vs. freq for fluxgaincal. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create fluxgaincal plot.')
                LOG.exception(ex)
                return None

        return wrapper


class modelfitSummaryChart(object):
    """
    Handles the creation of the "Flux vs frequency" figure in matplotlib
    """

    def __init__(self, context, result, webdicts):
        self.context = context
        self.result = result
        self.webdicts = webdicts
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        webdicts = self.webdicts
        plt.clf()

        mysize = 'small'
        # creates a range of colors evenly spaced along the color spectrum based on the number of sources to be plotted
        n = max(len(webdicts), 2)
        cmap = plt.colormaps["gist_rainbow"]
        colors = [cmap(i / (n - 1)) for i in range(n)]
        colorcount = 0

        fig = plt.figure(figsize=(10, 6))
        ax1 = fig.add_subplot(111)
        ax2 = ax1.twiny()

        dataminlist = []
        datamaxlist = []

        minfreqlist = []
        maxfreqlist = []

        for source, datadicts in webdicts.items():
            try:
                frequencies = []
                data = []
                model = []
                for datadict in datadicts:
                    data.append(float(datadict['data']))
                    model.append(float(datadict['fitteddata']))
                    frequencies.append(float(datadict['freq']))

                dataminlist.append(np.min(np.log10(data)))
                datamaxlist.append(np.max(np.log10(data)))

                frequencies = np.array(frequencies)
                minfreq = np.min(frequencies)
                maxfreq = np.max(frequencies)
                m = self.context.observing_run.get_ms(self.result.inputs['vis'])
                fieldobject = m.get_fields(source)
                if len(self.result.fluxscale_result) == 1:
                    fieldid = str([str(f.id) for f in fieldobject if str(f.id) in self.result.fluxscale_result[0].keys()][0])
                    spidx = self.result.fluxscale_result[0][fieldid]['spidx']
                    fitreff = self.result.fluxscale_result[0][fieldid]['fitRefFreq']
                else:
                    for single_fs_result in self.result.fluxscale_result:
                        try:
                            fieldid = str([str(f.id) for f in fieldobject if str(f.id) in single_fs_result.keys()][0])
                            spidx = single_fs_result[fieldid]['spidx']
                            fitreff = single_fs_result[fieldid]['fitRefFreq']
                        except Exception as e:
                            LOG.debug("Field error.")

                freqs = np.linspace(minfreq * 1.e9, maxfreq * 1.e9, 500)

                logfittedfluxd = np.zeros(len(freqs))
                for i in range(len(spidx)):
                    logfittedfluxd += spidx[i] * (np.log10(freqs / fitreff)) ** i

                fittedfluxd = 10.0 ** logfittedfluxd

                ax1.plot(np.log10(np.array(frequencies) * 1.e9), np.log10(data), 'o', label=source,
                         color=colors[colorcount])
                ax1.plot(np.log10(freqs), np.log10(fittedfluxd), '-', color=colors[colorcount])

                minfreqlist.append(minfreq)
                maxfreqlist.append(maxfreq)

                colorcount += 1

            except Exception as e:
                print(e)

        datamin = np.min(dataminlist)
        datamax = np.max(datamaxlist)

        # Set y-axis
        ylimlist = [datamin - (np.abs(datamin) * 0.2), datamax + (np.abs(datamax) * 0.2)]
        ax1.set_ylim(ylimlist)
        ax2.set_ylim(ylimlist)

        # Set x-axis
        ax1.tick_params(axis='x', which='minor', bottom=False)
        ax1.tick_params(bottom=True, top=False, left=True, right=False)
        ax1.tick_params(labelbottom=True, labeltop=False, labelleft=True, labelright=False)
        rangepad = (np.max(maxfreqlist) - np.min(minfreqlist)) * 0.1
        minxlim = 1.e9 * (np.min(minfreqlist) - rangepad)
        maxxlim = 1.e9 * (np.max(maxfreqlist) + rangepad)
        if rangepad > np.min(minfreqlist):
            minxlim = 1.e9 * np.min(minfreqlist) * 0.90
        ax1.set_xlim(np.log10(np.array([minxlim, maxxlim])))

        locs = ax1.get_xticks()
        locs = locs[1:-1]

        precision = 2
        labels = ["{:.{}f}".format(loc, precision) for loc in (10 ** locs) / 1.e9]

        ax2.set_xlim(np.log10(np.array([minxlim, maxxlim])))
        ax2.set_xticks(locs)
        ax2.set_xticklabels(labels)
        ax2.tick_params(bottom=False, top=True, left=False, right=False)
        ax2.tick_params(labelbottom=False, labeltop=True, labelleft=False, labelright=False)

        chartBox = ax1.get_position()
        ax1.set_position([chartBox.x0, chartBox.y0, chartBox.width * 0.6, chartBox.height])
        ax1.legend(loc='upper center', bbox_to_anchor=(1.45, 0.8), shadow=True, ncol=1)
        ax1.set_ylabel('log10 Flux Density [Jy]', size=mysize)
        ax1.set_xlabel('log10 Frequency [Hz]', size=mysize)
        ax2.set_xlabel('Frequency [GHz]', size=mysize)

        plt.savefig(figfile)
        plt.close()

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'modelfit-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='flux',
                              parameters={'vis': self.ms.basename,
                                          'type': 'fluxbootmodelfit',
                                          'figurecaption': 'Flux vs. frequency'})

        if not os.path.exists(figfile):
            LOG.trace('Plotting fluxboot fit vs. freq. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create fluxboot fitting plot.')
                LOG.exception(ex)
                return None

        return wrapper


class residualsSummaryChart(object):
    """
    Handles the creation of the "Fluxboot residuals vs. frequency" figure in matplotlib
    """

    def __init__(self, context, result, webdicts):
        self.context = context
        self.result = result
        self.webdicts = webdicts
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        webdicts = self.webdicts

        fig = plt.figure(figsize=(10, 6))
        ax1 = fig.add_subplot(111)
        ax2 = ax1.twiny()

        mysize = 'small'
        # creates a range of colors evenly spaced along the color spectrum based on the number of sources to be plotted
        n = max(len(webdicts), 2)
        cmap = plt.colormaps["gist_rainbow"]
        colors = [cmap(i / (n - 1)) for i in range(n)]
        colorcount = 0

        minfreqlist = []
        maxfreqlist = []

        for source, datadicts in webdicts.items():
            try:
                frequencies = []
                residuals = []
                for datadict in datadicts:
                    # PIPE-989, use fractional residuals rather than the absolute residuals.
                    residuals.append((float(datadict['data']) - float(datadict['fitteddata'])) / float(datadict['data']))
                    frequencies.append(float(datadict['freq']))

                frequencies = np.array(frequencies)
                minfreq = np.min(frequencies)
                maxfreq = np.max(frequencies)

                ax1.plot(np.log10(np.array(frequencies) * 1.e9), residuals, 'o', label=source, color=colors[colorcount])
                ax1.plot(np.linspace(np.min(np.log10(np.array(frequencies) * 1.e9)), np.max(np.log10(np.array(frequencies) * 1.e9)), 10),
                         np.zeros(10) + np.mean(residuals), linestyle='--', label='Mean', color=colors[colorcount])

                minfreqlist.append(minfreq)
                maxfreqlist.append(maxfreq)

                colorcount += 1

            except Exception as e:
                continue

        # Set x-axis
        ax1.tick_params(axis='x', which='minor', bottom=False)
        ax1.tick_params(bottom=True, top=False, left=True, right=False)
        ax1.tick_params(labelbottom=True, labeltop=False, labelleft=True, labelright=False)
        rangepad = (np.max(maxfreqlist) - np.min(minfreqlist)) * 0.1
        minxlim = 1.e9 * (np.min(minfreqlist) - rangepad)
        maxxlim = 1.e9 * (np.max(maxfreqlist) + rangepad)
        if rangepad > np.min(minfreqlist):
            minxlim = 1.e9 * np.min(minfreqlist) * 0.90
        ax1.set_xlim(np.log10(np.array([minxlim, maxxlim])))

        locs = ax1.get_xticks()
        locs = locs[1:-1]

        precision = 2
        labels = ["{:.{}f}".format(loc, precision) for loc in (10 ** locs) / 1.e9]

        ax2.set_xlim(np.log10(np.array([minxlim, maxxlim])))
        ax2.set_xticks(locs)
        ax2.set_xticklabels(labels)
        ax2.tick_params(bottom=False, top=True, left=False, right=False)
        ax2.tick_params(labelbottom=False, labeltop=True, labelleft=False, labelright=False)

        chartBox = ax1.get_position()
        ax1.set_position([chartBox.x0, chartBox.y0, chartBox.width * 0.6, chartBox.height])
        ax1.legend(loc='upper center', bbox_to_anchor=(1.45, 0.8), shadow=True, ncol=1)
        ax1.set_ylabel('Residuals ((data - fit) / data) [Jy]', size=mysize)
        ax1.set_xlabel('log10 Frequency [Hz]', size=mysize)
        ax2.set_xlabel('Frequency [GHz]', size=mysize)

        plt.savefig(figfile)
        plt.close()

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'residuals-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='residuals',
                              parameters={'vis': self.ms.basename,
                                          'type': 'fluxbootresiduals',
                                          'figurecaption': 'Fluxboot residuals vs. frequency'})

        if not os.path.exists(figfile):
            LOG.trace('Plotting fluxboot residuals vs. freq Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create fluxboot residuals plot.')
                LOG.exception(ex)
                return None

        return wrapper

