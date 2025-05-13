import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
from .basecleansequence import BaseCleanSequence

LOG = infrastructure.get_logger(__name__)


class AutoMaskThresholdSequence(BaseCleanSequence):

    def iteration(self, new_cleanmask=None, pblimit_image=-1, pblimit_cleanmask=-1, spw=None, frequency_selection=None,
                  iteration=None):

        if self.multiterm:
            extension = '.tt0'
        else:
            extension = ''

        if iteration is None:
            raise Exception('no data for iteration')

        elif iteration == 1:
            self.result.cleanmask = new_cleanmask
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter
        elif iteration == 2:
            if new_cleanmask != '' and self.flux not in (None, ''):
                # Make a circular one
                cm = casa_tools.image.newimagefromimage(infile=self.flux + extension, outfile=new_cleanmask,
                                                        overwrite=True)
                # verbose = False to suppress warning message
                cm.calcmask('T')
                cm.calc('1', verbose=False)
                cm.calc('replace("%s"["%s" > %f], 0)' %
                        (os.path.basename(new_cleanmask), self.flux+extension, pblimit_cleanmask), verbose=False)
                cm.calcmask('"%s" > %s' % (self.flux+extension, str(pblimit_image)))
                cm.done()

                if frequency_selection is not None:
                    channel_ranges = []
                    for spwid in spw.split(','):
                        spwkey = 'spw%s' % spwid
                        if spwkey in frequency_selection:
                            if frequency_selection[spwkey] not in (None, 'NONE', ''):
                                channel_ranges.extend(
                                    utils.freq_selection_to_channels(new_cleanmask,
                                                                     frequency_selection[spwkey].split()[0]))
                    if channel_ranges != []:
                        with casa_tools.ImageReader(new_cleanmask) as iaTool:
                            shape = iaTool.shape()
                            rgTool = casa_tools.regionmanager
                            for channel_range in channel_ranges:
                                LOG.info('Unmasking channels %d to %d' % (channel_range[0], channel_range[1]))
                                region = rgTool.box([0, 0, 0, channel_range[0]],
                                                    [shape[0]-1, shape[1]-1, 0, channel_range[1]])
                                iaTool.set(region=region, pixels=0.0, pixelmask=False)
                            rgTool.done()

            self.result.cleanmask = new_cleanmask
            # CAS-10489: old centralregion option needs higher threshold
            cqa = casa_tools.quanta
            self.result.threshold = '%sJy' % (cqa.getvalue(cqa.mul(self.threshold, 2.0))[0])
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter

        else:
            self.result.cleanmask = ''
            self.result.threshold = '0.0mJy'
            self.result.sensitivity = 0.0
            self.result.niter = 0

        return self.result
