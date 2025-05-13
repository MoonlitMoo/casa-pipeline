import pipeline.infrastructure as infrastructure
from .basecleansequence import BaseCleanSequence

LOG = infrastructure.get_logger(__name__)


class NoMaskThresholdSequence(BaseCleanSequence):

    def iteration(self, new_cleanmask='', pblimit_image=0.2, pblimit_cleanmask=0.3, spw=None, frequency_selection=None,
                  iteration=None):

        if iteration is None:
            raise Exception('no data for iteration')
        elif iteration == 1:
            self.result.cleanmask = ''
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter
        else:
            self.result.cleanmask = ''
            self.result.threshold = '0.0mJy'
            self.result.sensitivity = 0.0
            self.result.niter = 0

        return self.result
