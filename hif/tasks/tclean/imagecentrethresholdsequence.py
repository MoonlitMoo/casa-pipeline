import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
from .basecleansequence import BaseCleanSequence

LOG = infrastructure.get_logger(__name__)


class ImageCentreThresholdSequence(BaseCleanSequence):

    def iteration(self, new_cleanmask=None, pblimit_image=0.2, pblimit_cleanmask=0.3, spw=None,
                  frequency_selection=None, iteration=None):

        if self.multiterm:
            extension = '.tt0'
        else:
            extension = ''

        if iteration is None:
            raise Exception('no data for iteration')

        elif iteration == 1:
            # next iteration, 1, should have mask covering central area:
            #   flux > 0.3 (or adjusted for image size) when flux available
            #   centre quarter otherwise
            if self.flux not in (None, ''):
                cm = casa_tools.image.newimagefromimage(
                    infile=self.flux+extension, outfile=new_cleanmask, overwrite=True)
                # verbose = False to suppress warning message
                cm.calcmask('T')
                cm.calc('1', verbose=False)
                cm.calc('replace("%s"["%s" > %f], 0)' % (os.path.basename(new_cleanmask), self.flux+extension, pblimit_cleanmask), verbose=False)
                cm.calcmask('"%s" > %s' % (self.flux+extension, str(pblimit_image)))
                cm.done()
            else:
                cm = casa_tools.image.newimagefromimage(
                    infile=self.residuals[0]+extension, outfile=new_cleanmask, overwrite=True)
                cm.set(pixels='0')
                shape = cm.shape()
                rg = casa_tools.regionmanager
                region = rg.box([shape[0]//4, shape[1]//4],
                  [shape[0]-shape[0]//4, shape[1]-shape[1]//4])
                cm.set(pixels='1', region=region)
                rg.done()
                cm.done()

            if frequency_selection is not None:
                channel_ranges = []
                for spwid in spw.split(','):
                    spwkey = 'spw%s' % spwid
                    if spwkey in frequency_selection and frequency_selection[spwkey] not in (None, 'NONE', ''):
                        channel_ranges.extend(utils.freq_selection_to_channels(new_cleanmask, frequency_selection[spwkey].split()[0]))
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
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter
        else:
            self.result.cleanmask = ''
            self.result.threshold = '0.0mJy'
            self.result.sensitivity = 0.0
            self.result.niter = 0

        return self.result
