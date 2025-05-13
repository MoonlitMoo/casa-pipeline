import os

import matplotlib.pyplot as plt
import numpy as np
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.renderer.logger as logger

from mpl_toolkits.axes_grid1 import make_axes_locatable
from scipy.interpolate import griddata

LOG = infrastructure.get_logger(__name__)


class checkflagSummaryChart(object):
    def __init__(self, context, result, suffix='', plotms_args={}):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        self.plotms_args = plotms_args
        # self.caltable = result.final[0].gaintable

    def plot(self):
        plots = [None]

        if self.result.inputs['checkflagmode'] in ('bpd', 'bpd-vlass', 'bpd-vla'):
            plots = [self.get_plot_wrapper('BPcal'), self.get_plot_wrapper('delaycal')]
        if self.result.inputs['checkflagmode'] in ('allcals', 'allcals-vlass', 'allcals-vla'):
            plots = [self.get_plot_wrapper('allcals')]
        if self.result.inputs['checkflagmode'] == 'vlass-imaging':
            plots = [self.get_plot_wrapper('targets-vlass')]
        if self.result.inputs['checkflagmode'] == 'target-vla':
            plots = [self.get_plot_wrapper('targets')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):

        figfile = self.get_figfile(prefix)

        if self.suffix == 'before':
            vis = self.result.vis_averaged['before']
            vis_stats = self.result.vis_averaged['before_amp']
            amp_range = [vis_stats['']['min'], vis_stats['']['max']]
            title = 'Amp vs. Frequency (before flagging), {}'.format(prefix)
        if self.suffix == 'after':
            vis = self.result.vis_averaged['after']
            vis_stats = self.result.vis_averaged['before_amp']
            amp_range = [vis_stats['']['min'], vis_stats['']['max']]
            title = 'Amp vs. Frequency (after flagging), {}'.format(prefix)
        if self.suffix == 'after-autoscale':
            vis = self.result.vis_averaged['after']
            amp_range = [0., 0.]
            title = 'Amp vs. Frequency (after flagging, autoscale), {}'.format(prefix)

        # use the time-averged MS from the task to make the CASAplotms callmodify the plotms() call args.
        plotms_args = {'vis': vis,
                       'xaxis': 'freq', 'yaxis': 'amp',
                       'xdatacolumn': '', 'ydatacolumn': 'data',
                       'selectdata': True, 'field': '', 'scan': '', 'correlation': '',
                       'averagedata': False, 'avgtime': '1e8', 'avgscan': False,
                       'transform': False, 'extendflag': False,
                       'coloraxis': 'antenna2', 'plotrange': [0, 0, 0, 0],
                       'plotfile': figfile, 'overwrite': True, 'clearplots': True, 'showgui': False}
        plotms_args.update(self.plotms_args)
        plotms_args.update(title=title)
        amp_d = amp_range[1]-amp_range[0]
        plotms_args.update(plotrange=[0, 0, max(0, amp_range[0]-0.1*amp_d), amp_range[1]+0.1*amp_d])

        if prefix == 'BPcal':
            plotms_args.update(field=self.context.evla['msinfo'][self.ms.name].bandpass_field_select_string)
            plotms_args.update(scan=self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string)
        if prefix == 'delaycal':
            plotms_args.update(scan=self.context.evla['msinfo'][self.ms.name].delay_scan_select_string)

        LOG.info('Creating {}'.format(plotms_args['title']))

        # run plotms
        job = casa_tasks.plotms(**plotms_args)
        job.execute()

        return plotms_args

    def get_figfile(self, prefix):
        stage_dir = os.path.join(self.context.report_dir, 'stage{}'.format(self.result.stage_number))
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)
        fig_basename = '-'.join(list(filter(None, ['checkflag', prefix,
                                                   self.ms.basename, 'summary', self.suffix])))+'.png'
        return os.path.join(stage_dir, fig_basename)

    def get_plot_wrapper(self, prefix):

        if prefix == 'delaycal':
            bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
            delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string
            if bandpass_scan_select_string == delay_scan_select_string:
                return None

        figfile = self.get_figfile(prefix)
        if not os.path.exists(figfile):
            LOG.trace('Checkflag summary plot not found. Creating new plot.')
            try:
                plotms_args = self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                              parameters={'vis': self.ms.basename,
                                          'type': prefix,
                                          'version': self.suffix,
                                          'plotms_args': plotms_args,
                                          'spw': ''})
        return wrapper


class checkflagPercentageMap(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.figfile = self._get_figfile()

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing checkflag percentage map plot')
            return self._get_plot_object()

        LOG.info('Creating new checkflag percentage map plot')
        try:

            fig_title = self.ms.basename

            fig, ax = plt.subplots()
            fields_name, fields_ra, fields_dec = self._fields_to_ra_dec()

            flags_by_field = self.result.summaries[-1]['field']
            flagpct_per_field = np.zeros((len(flags_by_field), 3))

            for idx, (field_name, field_flag) in enumerate(flags_by_field.items()):
                field_id = fields_name.index(field_name)
                flagpct_per_field[idx, 0] = 100.0 * field_flag['flagged']/field_flag['total']
                flagpct_per_field[idx, 1] = np.degrees(float(fields_ra[field_id]))
                flagpct_per_field[idx, 2] = np.degrees(float(fields_dec[field_id]))

            self._plot_grid(flagpct_per_field[:, 1], flagpct_per_field[:, 2], flagpct_per_field[:, 0])
            ax.set_xlabel('R.A. [deg]')
            ax.set_ylabel('Dec. [deg]')

            fig.savefig(self.figfile, bbox_inches='tight')
            plt.close(fig)

        except Exception as ex:
            LOG.warning('Exception: Could not create the flagging percentage map plot. {!s}'.format(str(ex)))
            return None

        return self._get_plot_object()

    def _get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'checkflag-%s-percentagemap.png' % self.ms.basename)

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis='R.A.',
                           y_axis='Dec.',
                           parameters={'vis': self.ms.basename})

    def _fields_to_ra_dec(self):

        with casa_tools.TableReader(self.ms.name + '/FIELD') as table:
            phase_dir = table.getcol('PHASE_DIR')
            field_names = table.getcol('NAME')

        if len(phase_dir.shape) > 2:
            phase_dir = phase_dir.squeeze()

        return list(field_names), phase_dir[0, :], phase_dir[1, :]

    def _plot_grid(self, x, y, z, nx=200, ny=200):

        x[x < 0] = x[x < 0] + 360.

        xi, dx = np.linspace(np.max(x), np.min(x), nx, retstep=True)
        yi, dy = np.linspace(np.min(y), np.max(y), ny, retstep=True)
        zi = griddata((x, y), z, (xi[None, :], yi[:, None]), method='cubic')
        with np.errstate(invalid='ignore'):
            zi[zi > 100] = 100.

        ax = plt.gca()
        im = ax.imshow(zi, origin='lower', extent=[np.max(x)-0.5*dx, np.min(x) + 0.5*dx,
                                                   np.min(y)-0.5*dy, np.max(y)+0.5*dy], aspect='equal')
        ax.plot(x, y, 'k.')

        dxy = min((np.max(x)-np.min(x))*0.05, (np.max(y)-np.min(y))*0.05)
        ax.set_xlim(np.max(x)+dxy, np.min(x) - dxy)
        ax.set_ylim(np.min(y)-dxy, np.max(y)+dxy)
        ax.get_xaxis().get_major_formatter().set_useOffset(False)
        cax = make_axes_locatable(ax).append_axes("right", size="5%", pad=0.05)

        cba = plt.colorbar(im, cax=cax)
        cba.set_label('percent flagged [%]')
