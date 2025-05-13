import pipeline.infrastructure as infrastructure

from .basecleansequence import BaseCleanSequence

LOG = infrastructure.get_logger(__name__)


class AutoScalThresholdSequence(BaseCleanSequence):
    """Not implemented yet."""

    def iteration(self, new_cleanmask=None, pblimit_image=-1, pblimit_cleanmask=-1, spw=None, frequency_selection=None,
                  iteration=None):

        if self.multiterm:
            extension = '.tt0'
        else:
            extension = ''

        if iteration is None:
            raise Exception('no data for iteration')

        elif (iteration == 1 or iteration == 2 or iteration == 3):
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
