import os
import collections

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks

LOG = infrastructure.get_logger(__name__)


class plotsummarySummaryChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

    def plot(self):
        plots = []
        context = self.context

        m = context.observing_run.measurement_sets[0]
        corrstring = m.get_vla_corrstring()
        calibrator_field_select_string = context.evla['msinfo'][m.name].calibrator_field_select_string
        channels = m.get_vla_numchan()

        ms_active = m.name

        # create phase time plot for all calibrators
        figfile = self.get_figfile('all_calibrators_phase_time')

        plot = logger.Plot(figfile, x_axis='time', y_axis='phase',
                           parameters={'vis': self.ms.basename,
                                       'type': 'All calibrators',
                                       'bandname': 'All bands',
                                       'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('Plotting phase vs. time for all calibrators. Creating new plot.')
            try:
                job = casa_tasks.plotms(vis=ms_active, xaxis='time', yaxis='phase', ydatacolumn='corrected',
                                        selectdata=True, field=calibrator_field_select_string, correlation=corrstring,
                                        averagedata=True, avgchannel=str(max(channels)), avgtime='1e8', avgscan=False,
                                        transform=False, extendflag=False, iteraxis='', coloraxis='antenna2',
                                        plotrange=[], title='Calibrated phase vs. time, all calibrators',
                                        xlabel='',  ylabel='', showmajorgrid=False, showminorgrid=False,
                                        plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

                job.execute()

            except Exception as ex:
                LOG.error('Could not create plotsummary plot.')
                LOG.exception(ex)
                plot = None

        plots.append(plot)

        # create amp vs. UVwave plots of each cal field and then max 30 targets
        calfields = m.get_fields(intent='BANDPASS,PHASE,AMPLITUDE,POLARIZATION,POLANGLE,POLLEAKAGE')
        alltargetfields = m.get_fields(intent='TARGET')

        plotfields = calfields

        nplots = (len(alltargetfields)//30)+1

        targetfields = [field for field in alltargetfields[0:len(alltargetfields):nplots]]

        plotfields.extend(targetfields)

        # Make plots per band
        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        # Add the "All" band key/value if the dataset is multi-band.
        if len(band2spw) > 1:
            band2spw['All'] = [spw for spws_per_band in band2spw.values() for spw in spws_per_band]

        for field in plotfields:
            plots_perband = []
            spws_perband = []
            for bandname, spwlist in band2spw.items():
                figfile = self.get_figfile('field'+str(field.id)+'_amp_uvdist_{!s}'.format(bandname))

                source_spwobjlist = list(field.valid_spws)
                source_spwidlist = [spw.id for spw in source_spwobjlist]
                source_spwidlist.sort()
                # Find if spwlist contents are in the valid_spws for this band **and** field
                spwidlist = [int(spw) for spw in spwlist]
                spwuselist = [str(spwid) for spwid in spwidlist if spwid in source_spwidlist]

                spwuse_str = ','.join(spwuselist)
                # skip if the same spw combination has been plotted before (likley this specific field was observed in a single band)
                if spwuse_str in spws_perband:
                    break
                spws_perband.extend(spwuse_str)

                if spwuselist:
                    LOG.info(bandname+': ' + ','.join(spwuselist) + '    Field: '+field.name + '   id:'+str(field.id))

                    plot = logger.Plot(figfile, x_axis='uvwave', y_axis='amp',
                                       parameters={'vis': self.ms.basename,
                                                   'type': 'Field '+str(field.id)+', '+field.name,
                                                   'field': str(field.id),
                                                   'bandname': bandname,
                                                   'spw': spwuse_str})

                    if not os.path.exists(figfile):
                        LOG.info('Plotting amp vs. uvwave for field id='+str(field.id) +
                                 '  Band '+bandname+'.  Creating new plot.')

                        try:
                            job = casa_tasks.plotms(vis=ms_active, xaxis='uvwave', yaxis='amp', ydatacolumn='corrected',
                                                    selectdata=True, field=str(field.id), correlation=corrstring,
                                                    spw=spwuse_str,
                                                    averagedata=True, avgchannel=str(max(channels)), avgtime='1e8',
                                                    avgscan=False, transform=False, extendflag=False, iteraxis='',
                                                    coloraxis='spw', plotrange=[],
                                                    title='Field '+str(field.id)+', '+field.name +
                                                    '   Band ' + bandname,
                                                    xlabel='', ylabel='',  showmajorgrid=False, showminorgrid=False,
                                                    plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

                            job.execute()

                        except Exception as ex:
                            LOG.error('Could not create plot for field {!s}  band {!s}'.format(str(field.id), bandname))
                            LOG.exception(ex)
                            plot = None
                    if bandname == 'All':
                        # put the "All" band plot ahead of single-band plots.
                        plots_perband.insert(0, plot)
                    else:
                        plots_perband.append(plot)

            plots.extend(plots_perband)

        return [p for p in plots if p is not None]

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            prefix+'-%s-summary.png' % self.ms.basename)
