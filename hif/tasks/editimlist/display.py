import os

import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.axes_grid1 import make_axes_locatable

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.infrastructure.displays.plotstyle import matplotlibrc_formal

LOG = infrastructure.get_logger(__name__)


class VlassFlagSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

    @matplotlibrc_formal
    def plot(self):

        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        plot_wrappers = []

        vlass_flag_stats = self.result.vlass_flag_stats

        spwgroup_list = vlass_flag_stats['spwgroup_list']
        flagpct_thresh = vlass_flag_stats['flagpct_thresh']
        nfield_thresh = vlass_flag_stats['nfield_thresh']
        scan_list = vlass_flag_stats['scan_list']
        nfield_above_flagpct = vlass_flag_stats['nfield_above_flagpct']
        fname_list = vlass_flag_stats['fname_list']
        flagpct_field_spwgroup = vlass_flag_stats['flagpct_field_spwgroup']
        spwgroup_reject = vlass_flag_stats['spwgroup_reject']

        scan_idx = []
        scan_label = []
        scan_edge = []
        for scan_unique in np.unique(scan_list):
            field_idxs = np.where(scan_list == scan_unique)[0]
            scan_idx.append(np.min(field_idxs))
            scan_edge.append((np.min(field_idxs)-0.5, np.max(field_idxs)+0.5))
            scan_desc = [fname_list[field_idxs[0]],
                         f'scan no.: {int(scan_list[field_idxs[0]])}']
            scan_label.append('\n'.join(scan_desc))

        figfile = os.path.join(stage_dir, 'vlass_flagsummary_field_spwgroup.png')
        n_field, n_spwgroup = flagpct_field_spwgroup.shape

        try:

            fig, ax = plt.subplots(figsize=(10, 10))

            im = ax.imshow(flagpct_field_spwgroup*100., origin='lower', aspect='auto',
                           extent=(-0.5, n_spwgroup-0.5, -0.5, n_field-0.5))

            ax.set_xticks(np.arange(n_spwgroup))
            xticklabels = []
            for idx, spwgroup in enumerate(spwgroup_list):
                if spwgroup_reject[idx]:
                    reject_str = ' (r)'
                else:
                    reject_str = ''
                xticklabels.append(f'{spwgroup}{reject_str}\n n={nfield_above_flagpct[idx]}')
            ax.set_xticklabels(xticklabels)

            xticklabels = ax.get_xticklabels()
            for idx, reject in enumerate(spwgroup_reject):
                if reject:
                    xticklabels[idx].set_color('red')
            with plt.rc_context({'mathtext.default':  'regular'}):
                ax.set_xlabel(
                    f'Spw Selection\n n$_\mathdefault{{field}}$ (flagpct>flagpct$_\mathdefault{{th}}$): n$_\mathdefault{{th}}$={nfield_thresh}, flagpct$_\mathdefault{{th}}$={flagpct_thresh*100}%')
            ax.set_ylabel('VLASS Image Row: 1st field name')
            ax.tick_params(which='minor', bottom=False, left=False)

            ax.set_xticks(np.arange(n_spwgroup+1)-0.5, minor=True)
            ax.set_yticks(np.arange(n_field+1)-0.5, minor=True)

            ax.set_yticks(np.unique(scan_idx), minor=False)
            ax.set_yticklabels(scan_label, rotation=45, ma='left', va='center', rotation_mode="anchor")

            ax.grid(which='minor', axis='both', color='white', linestyle='-', linewidth=2)
            ax.set_title('Flagged fraction per field')

            cax = make_axes_locatable(ax).append_axes("right", size="5%", pad=0.05)
            cba = plt.colorbar(im, cax=cax)
            cba.set_label('percent flagged [%]')

            fig.tight_layout()
            fig.savefig(figfile, bbox_inches='tight')
            plt.close(fig)

            plot = logger.Plot(figfile,
                               x_axis='Spw Group',
                               y_axis='Field',
                               parameters={})

            plot_wrappers.append(plot)

        except Exception as ex:
            LOG.warning('Could not create plot %s', figfile)
            LOG.warning(ex)

        return plot_wrappers
