#!/usr/bin/env python

import os
import numpy as np
import matplotlib.pyplot as plt

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

def plot_beams(psf_name, plotfile):
    """
    Method to plot per channel beams from a PSF cube.
    """

    qaTool = casa_tools.quanta

    with casa_tools.ImageReader(psf_name) as image:
        im_info = image.summary()

    f_index = np.where(im_info['axisnames'] == 'Frequency')[0][0]
    f_unit = im_info['axisunits'][f_index]

    num_chan = im_info['shape'][f_index]

    channels = np.arange(num_chan)
    freq_refval = float(qaTool.getvalue(qaTool.convert(qaTool.quantity(im_info['refval'][f_index], f_unit), 'GHz')))
    freq_refchan = float(qaTool.getvalue(qaTool.convert(qaTool.quantity(im_info['refpix'][f_index], f_unit), 'GHz')))
    freq_step = float(qaTool.getvalue(qaTool.convert(qaTool.quantity(im_info['incr'][f_index], f_unit), 'GHz')))
    freqs = np.array([freq_refval+freq_step*(c-freq_refchan) for c in channels])

    rb = im_info['perplanebeams']
    major = np.array([float(qaTool.getvalue(qaTool.convert(rb['beams'][f'*{c}']['*0']['major'], 'arcsec'))) for c in channels])
    minor = np.array([float(qaTool.getvalue(qaTool.convert(rb['beams'][f'*{c}']['*0']['minor'], 'arcsec'))) for c in channels])

    plt.close('all')
    fig, ax = plt.subplots(figsize=(6.4, 4.8), sharex=True)
    plt.text(0.5, 1.18, 'Beam size per channel', transform=ax.transAxes, fontsize=12, ha='center')
    plt.text(0.5, 1.14, os.path.basename(psf_name), transform=ax.transAxes, fontsize=9, ha='center')
    ax.plot(freqs, major, label='Major axis', c='darkblue')
    ax.plot(freqs, minor, label='Minor axis', c='orange')
    ax.legend(loc='center left')
    ax.set_xlabel('Frequency [GHz]')
    ax.set_ylabel('Axis size [arcsec]')

    # Alternative x axis
    ax2y = ax.twiny()
    ax2y.set_xlim(map(lambda f: (f-freqs[0])/freq_step, ax.get_xlim()))
    ax2y.set_xlabel(f'{num_chan} Channels')

    # Alternative y axis
    ax2x = ax.twinx()
    ax2x.set_ylim(ax.get_ylim() / max(major))
    ax2x.set_ylabel('Axis size / max(Major axis)')

    fig.savefig(plotfile, bbox_inches='tight')
    plt.clf()
    plt.close(fig)
