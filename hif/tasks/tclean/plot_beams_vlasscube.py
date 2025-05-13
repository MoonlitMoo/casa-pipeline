import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter

from pipeline.infrastructure.displays.plotstyle import matplotlibrc_formal


@matplotlibrc_formal
def plot_beams_vlasscube(vlass_cube_metadata, figfile='beams_vlasscube.png', logscale=False):
    """Create the VLASS Cube plane rejection summary plot."""

    freq_list = vlass_cube_metadata['freq_list']
    bmajor_list = vlass_cube_metadata['bmajor_list']
    bminor_list = vlass_cube_metadata['bminor_list']
    spwgroup_list = vlass_cube_metadata['spwgroup_list']
    flagpct_list = vlass_cube_metadata['flagpct_list']
    plane_keep = vlass_cube_metadata['plane_keep']
    beam_dev = vlass_cube_metadata['beam_dev']
    flagpct_threshold = vlass_cube_metadata['flagpct_threshold']
    ref_idx = vlass_cube_metadata['ref_idx']

    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(8, 6), gridspec_kw={'height_ratios': [3, 2]})
    ax.plot(freq_list, bmajor_list, label=r'$\rm Bmaj_{rej.}$', c='darkblue', marker='o', linestyle='None', fillstyle='none')
    ax.plot(freq_list, bminor_list, label=r'$\rm Bmin_{rej.}$', c='green', marker='o', linestyle='None', fillstyle='none')
    ax.plot(freq_list[plane_keep], bmajor_list[plane_keep], c='darkblue', marker='o', linestyle='None')
    ax.plot(freq_list[plane_keep], bminor_list[plane_keep], c='green', marker='o', linestyle='None')
    ax.plot(freq_list[ref_idx], bmajor_list[ref_idx], label=r'$\rm Bmaj_{ref}$', c='darkblue', marker='s', linestyle='None')
    ax.plot(freq_list[ref_idx], bminor_list[ref_idx], label=r'$\rm Bmin_{ref}$', c='green', marker='s', linestyle='None')

    freq_scaled = np.arange(min(freq_list)-0.5, max(freq_list)+0.5, 0.1)
    bmajor_scaled = bmajor_list[ref_idx]*freq_list[ref_idx]/freq_scaled
    bminor_scaled = bminor_list[ref_idx]*freq_list[ref_idx]/freq_scaled

    dfreq = 0.1
    if len(freq_list) > 1:
        dfreq = abs(freq_list[1]-freq_list[0])*0.5

    ax2.bar(freq_list, flagpct_list, color='gray', alpha=1.0, width=dfreq, label=spwgroup_list)
    ax2.axhline(y=flagpct_threshold, color='red', linestyle='dashed')
    ax2.set_ylabel('Flagged Pct.')
    xmin, xmax = ax2.get_xlim()

    ax2.set_xticks(freq_list, minor=False)

    xticklabels = []
    for idx, spwgroup in enumerate(spwgroup_list):
        if not plane_keep[idx]:
            reject_str = ' (r)'
        else:
            reject_str = ''
        xticklabels.append(f'{spwgroup}{reject_str}')
    ax2.set_xticklabels(xticklabels)
    xticklabels = ax2.get_xticklabels()
    for idx, xticklabel in enumerate(xticklabels):
        if not plane_keep[idx]:
            xticklabel.set_color('red')

    ax.set_xlim(xmin, xmax)
    ax.plot(freq_scaled, bmajor_scaled, label=r'$\rm Bmaj_{scaled}$', c='darkblue', linestyle='dashed')
    ax.fill_between(freq_scaled, bmajor_scaled*(1.-beam_dev), bmajor_scaled*(1.+beam_dev), color='darkblue', alpha=0.2)
    ax.plot(freq_scaled, bminor_scaled, label=r'$\rm Bmin_{scaled}$', c='green', linestyle='dashed')
    ax.fill_between(freq_scaled, bminor_scaled*(1.-beam_dev), bminor_scaled*(1.+beam_dev), color='green', alpha=0.2)

    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.25),
              fancybox=False, shadow=False, ncol=3, frameon=False)

    ax.set_xlabel('Ref. Freq. [GHz]')
    ax2.set_xlabel('Spw Selection')

    with plt.rc_context({'mathtext.default':  'regular'}):
        ax2.set_xlabel(
            f'Spw Selection\n BeamDev$_\mathdefault{{th}}$={beam_dev}, flagpct$_\mathdefault{{th}}$={flagpct_threshold*100}%')

    ax.set_ylabel('Beam Size [arcsec]')

    if logscale:
        ax.set_yscale("log")
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(ScalarFormatter())
        ax.xaxis.set_minor_formatter(ScalarFormatter())
        ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.yaxis.set_minor_formatter(ScalarFormatter())
        ax.yaxis.set_minor_locator(plt.MultipleLocator(2))

    fig.tight_layout()
    fig.savefig(figfile, bbox_inches='tight')
    plt.close(fig)
