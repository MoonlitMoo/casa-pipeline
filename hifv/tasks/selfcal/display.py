import os

import matplotlib as mpl
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.renderer.logger as logger

#from astropy import units as u
#from astropy.coordinates import SkyCoord

LOG = infrastructure.get_logger(__name__)


class selfcalphaseGainPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir, 'stage%s' % result.stage_number,
                                          'selfcalphasegain-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting phase/SNR vs. time after running hifv_selfcal")

        snr_plotrange = [0, 0, 0, 5.0]
        with casa_tools.TableReader(result.caltable) as table:
            snr = table.getcol('SNR')
            flag = table.getcol('FLAG')
        if np.sum(flag) < flag.size:
            snr_plotrange[-1] = 1.1*np.nanmax(snr[~flag])

        for ii in range(nplots):

            filename = 'selfcalgainphase' + str(ii) + '.png'
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
                    LOG.debug("Plotting phase/SNR vs. time... {!s}".format(antName))
                    job = casa_tasks.plotms(vis=result.caltable, xaxis='time', yaxis='phase', field='',
                                            antenna=antPlot, spw='', timerange='',
                                            gridrows=2, gridcols=1, rowindex=0, colindex=0, plotindex=0,
                                            coloraxis='', plotrange=[0, 0, -180, 180], symbolshape='circle',
                                            title='G table: {!s}   Antenna: {!s}'.format(result.caltable, antName), xlabel=' ',
                                            showlegend=False,
                                            showgui=False, plotfile=figfile, clearplots=True, overwrite=True,
                                            titlefont=8, xaxisfont=7, yaxisfont=7, xconnector='line')
                    job.execute()

                    job = casa_tasks.plotms(vis=result.caltable, xaxis='time', yaxis='snr', field='',
                                            antenna=antPlot, spw='', timerange='',
                                            gridrows=2, gridcols=1, rowindex=1, colindex=0, plotindex=1,
                                            coloraxis='', plotrange=snr_plotrange, symbolshape='circle',
                                            title=' ',
                                            showgui=False, plotfile=figfile, clearplots=False, overwrite=True,
                                            showlegend=False,
                                            titlefont=8, xaxisfont=7, yaxisfont=7, xconnector='line')
                    job.execute()

                except Exception as ex:
                    LOG.warning("Unable to plot " + filename + str(ex))
            else:
                LOG.debug('Using existing ' + filename + ' plot.')

            try:
                plot = logger.Plot(figfile, x_axis='Time', y_axis='Phase', field='',
                                   parameters={'spw': '',
                                               'pol': '',
                                               'ant': antName,
                                               'type': 'selfcalphasegain',
                                               'file': os.path.basename(figfile)})
                plots.append(plot)
            except Exception as ex:
                LOG.warning("Unable to add plot to stack.  " + str(ex))
                plots.append(None)

        return [p for p in plots if p is not None]


class selfcalSolutionNumPerFieldChart(object):
    """present the selfcal solution flag stats as a heatmap.
    likely only work for the self-cal gain table from the 'VLASS-SE' mode (see PIPE-1010), i.e.
        one solution per polarization per image-row (unique source id) for each antenna
    """

    def __init__(self, context, result):
        self.context = context
        self.caltable = result.caltable
        self.reportdir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
        self.figfile = self._get_figfile()
        self.x_axis = 'Antenna'
        self.y_axis = 'VLASS image row '

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing selfcal solution summary plot')
            return self._get_plot_object()

        selfcal_stats = self._calstat_from_caltable()
        LOG.debug('Creating new selfcal solution summary plot')

        try:

            fig, ax = plt.subplots(figsize=(10, 8))

            n_ant = len(selfcal_stats['ant_unique_id'])
            n_row = len(selfcal_stats['field_unique_id'])

            flag2d = selfcal_stats['flag2d'].reshape((-1, n_ant), order='F')
            cmap = mpl.colors.ListedColormap(['limegreen', 'blue', 'gray'])
            norm = mpl.colors.BoundaryNorm(np.arange(cmap.N+1)-0.5, cmap.N)
            ax.imshow(flag2d, origin='lower', extent=(-0.5, n_ant-0.5, -
                                                      0.5, n_row-0.5), cmap=cmap, norm=norm, aspect='auto')

            ant_name = selfcal_stats['ant_desc']['name']
            field_name = selfcal_stats['field_desc']['name']

            ant_label = ant_name[selfcal_stats['ant_unique_id']]
            row_label = field_name[selfcal_stats['field_unique_id']]

            row_label = []
            for idx, field_id in enumerate(selfcal_stats['field_unique_id']):
                # alternatively, you could include RA/Dec in y-axis labels
                # c = selfcal_stats['field_desc']['position'][field_id]
                # row_desc = [field_name[field_id],
                #            c.ra.to_string(unit=u.hour, pad=True, precision=1),
                #            c.dec.to_string(unit=u.degree, pad=True, precision=0, alwayssign=True)]
                row_desc = [field_name[field_id],
                            'scan no.: {}'.format(selfcal_stats['field_unique_scan'][idx])]
                row_label.append('\n'.join(row_desc))

            ax.set_xticks(np.arange(len(ant_label)))
            ax.set_xticklabels(ant_label, rotation=45, ha='right', rotation_mode='anchor')
            ax.set_yticks(np.arange(len(row_label)))
            ax.set_yticklabels(row_label, rotation=45, ma='left', va='center', rotation_mode="anchor")
            ax.grid(which='major', axis='y', color='white', linestyle='-', linewidth=2)

            # for spine in ax.spines:
            #   ax.spines[spine].set_visible(False)

            ax.set_xticks(np.arange(len(ant_label)+1)-0.5, minor=True)
            ax.set_yticks(np.arange(len(row_label)+1)-0.5, minor=True)
            ax.grid(which='minor', axis='both', color='white', linestyle='-', linewidth=10)

            ax.tick_params(which='minor', bottom=False, left=False)

            ax.set_title('Selfcal Solution Flags')
            ax.set_ylabel('VLASS Image Row: 1st field name')

            lg_colors = {'unflagged': 'limegreen', 'flagged': 'blue', 'ref.ant.': 'gray'}
            lg_patch = [mpatches.Patch(color=lg_colors[lg_label], label=lg_label) for lg_label in lg_colors]
            ax.legend(handles=lg_patch, bbox_to_anchor=(0.5, -0.1), loc='upper center', ncol=len(lg_patch))
            fig.tight_layout()
            fig.savefig(self.figfile)
            plt.close(fig)

            LOG.debug('Saving new iselfcal solution summary plot to {}'.format(self.figfile))

        except:

            LOG.debug('Unable to create {}'.format(self.figfile))
            return None

        return self._get_plot_object()

    def _get_figfile(self):

        return os.path.join(self.reportdir,
                            self.caltable+'.nsols.png')

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis=self.x_axis,
                           y_axis=self.y_axis)

    def _calstat_from_caltable(self):
        """get selfcal solution statistics from caltable.
        """
        with casa_tools.TableReader(self.caltable) as table:
            # expected row number: n_ant x n_vlass_row
            time = table.getcol('TIME')
            field_id = table.getcol('FIELD_ID')
            cparam = table.getcol('CPARAM')  # (n_pol x n_chan x n_row)
            flag = table.getcol('FLAG')
            snr = table.getcol('SNR')
            ant1_id = table.getcol('ANTENNA1')
            ant2_id = table.getcol('ANTENNA2')
            scan_no = table.getcol('SCAN_NUMBER')

        with casa_tools.TableReader(self.caltable+'/FIELD') as table:
            field_name = table.getcol('NAME')
            phasedir = table.getcol('PHASE_DIR')

        with casa_tools.TableReader(self.caltable+'/ANTENNA') as table:
            ant_name = table.getcol('NAME')

        field_unique_id, field_unique_idx1st, field_unique_inverse = np.unique(
            field_id, return_index=True, return_inverse=True)
        ant_unique_id, ant_unique_idx1st, ant_unique_inverse = np.unique(
            ant1_id, return_index=True, return_inverse=True)
        field_unique_scan = scan_no[field_unique_idx1st]

        # alternatively, you could group solution entries by their field id
        # field_list = np.split(field_id, field_unique_idx1st[1:], axis=-1)
        # flag_list = np.split(flag, field_unique_idx1st[1:], axis=-1)
        # ant_list = np.split(ant1_id, field_unique_idx1st[1:], axis=-1)
        # time_list = np.split(time, field_unique_idx1st[1:], axis=-1)

        if len(phasedir.shape) > 2:
            phasedir = phasedir.squeeze()

        # sort field_unique_ids/scans by their dec.
        field_sort_idx = np.argsort(phasedir[1, field_unique_id])
        field_unique_id = field_unique_id[field_sort_idx]
        field_unique_scan = field_unique_scan[field_sort_idx]
        field_unique_inverse = field_sort_idx[field_unique_inverse]

        n_ant = len(ant_unique_id)
        n_field = len(field_unique_id)

        # flag2d: 0: unflagged solution; 1: solution flagged; 2: reference antenna
        # note: flag2d actually has four dimensions: (n_pol, n_chan, n_field, n_ant)
        flag2d = np.zeros(flag.shape[:-1]+(n_field, n_ant))
        for idx in range(len(field_id)):
            flag2d[:, :, field_unique_inverse[idx], ant_unique_inverse[idx]] = flag[:, :, idx]
            if ant1_id[idx] == ant2_id[idx]:
                flag2d[:, :, field_unique_inverse[idx], ant_unique_inverse[idx]] = 2

        selfcal_stats = {'field_unique_id': field_unique_id,
                         'field_unique_scan': field_unique_scan,
                         'ant_unique_id': ant_unique_id,
                         'flag2d': flag2d,
                         #'field': field_list,'flag': flag_list,'antenna': ant_list,'time': time_list,
                         #'field_desc': {'name': field_name, 'position': SkyCoord(ra=phasedir[0, :]*u.rad, dec=phasedir[1, :]*u.rad)},
                         'field_desc': {'name': field_name},
                         'ant_desc': {'name': ant_name},
                         'column': {'cparam': cparam, 'flag': flag, 'snr': snr, 'ant1': ant1_id, 'ant2': ant2_id}}

        return selfcal_stats
