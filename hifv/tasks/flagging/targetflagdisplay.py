import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


class targetflagSummaryChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

    def plot(self):
        plots = []

        m = self.context.observing_run.measurement_sets[0]

        corrstring = m.get_vla_corrstring()
        calibrator_field_select_string = self.context.evla['msinfo'][m.name].calibrator_field_select_string
        with casa_tools.TableReader(m.name+'/FIELD') as table:
            numfields = table.nrows()
            field_ids = list(range(numfields))
            field_names = table.getcol('NAME')

        channels = m.get_vla_numchan()

        # create phase time plot for all calibrators
        figfile = self.get_figfile('all_calibrators_phase_time')

        plot = logger.Plot(figfile, x_axis='time', y_axis='phase',
                           parameters={'vis': self.ms.basename,
                                       'type': 'All calibrators',
                                       'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('Plotting phase vs. time for all calibrators. Creating new '
                      'plot.')
            try:
                job = casa_tasks.plotms(vis=m.name, xaxis='time', yaxis='phase', ydatacolumn='corrected',
                                        selectdata=True, field=calibrator_field_select_string, correlation=corrstring,
                                        averagedata=True, avgchannel=str(max(channels)), avgtime='1e8', avgscan=False,
                                        transform=False, extendflag=False, iteraxis='', coloraxis='antenna2',
                                        plotrange=[], title='Calibrated phase vs. time, all calibrators',
                                        xlabel='',  ylabel='', showmajorgrid=False, showminorgrid=False,
                                        plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

                job.execute()

            except Exception as ex:
                LOG.error('Could not create fluxboot plot.')
                LOG.exception(ex)
                plot = None

        plots.append(plot)

        # create amp vs. UVwave plots of each field
        for ii in field_ids:
            figfile = self.get_figfile('field'+str(field_ids[ii])+'_amp_uvdist')

            plot = logger.Plot(figfile, x_axis='uvwave', y_axis='amp',
                               parameters={'vis': self.ms.basename,
                                           'type': 'Field '+str(field_ids[ii])+', '+field_names[ii],
                                           'field': str(field_ids[ii]),
                                           'spw': ''})

            if not os.path.exists(figfile):
                LOG.trace('Plotting amp vs. uvwave for field id='+str(field_ids[ii])+'.  Creating new plot.')

                try:
                    job = casa_tasks.plotms(vis=m.name,  xaxis='uvwave',  yaxis='amp',  ydatacolumn='corrected',
                                            selectdata=True, field=str(field_ids[ii]), correlation=corrstring,
                                            averagedata=True, avgchannel=str(max(channels)), avgtime='1e8',
                                            avgscan=False, transform=False, extendflag=False, iteraxis='',
                                            coloraxis='spw', plotrange=[],
                                            title='Field '+str(field_ids[ii])+', '+field_names[ii],
                                            xlabel='', ylabel='', showmajorgrid=False, showminorgrid=False,
                                            plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

                    job.execute()

                except Exception as ex:
                    LOG.error('Could not create plot for field '+str(field_ids[ii]))
                    LOG.exception(ex)
                    plot = None

            plots.append(plot)

        return [p for p in plots if p is not None]

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            prefix+'-%s-summary.png' % self.ms.basename)
