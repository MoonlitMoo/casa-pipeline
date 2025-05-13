import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import casa_tasks
from .basecleansequence import BaseCleanSequence

LOG = infrastructure.get_logger(__name__)


class VlassMaskThresholdSequence(BaseCleanSequence):

    def __init__(self, multiterm=None, gridder='', threshold='0.0mJy', sensitivity=0.0, niter=0, mask='',
                 channel_rms_factor=1.0, executor=None):
        """Constructor.
        """
        BaseCleanSequence.__init__(self, multiterm, gridder, threshold, sensitivity, niter)
        self.mask = mask
        self.channel_rms_factor = channel_rms_factor
        self.__executor = executor

    def iteration(self, new_cleanmask=None, pblimit_image=-1, pblimit_cleanmask=-1, spw=None, frequency_selection=None,
                  iteration=None):

        if iteration is None:
            raise Exception('no data for iteration')

        if iteration in [1, 2] and new_cleanmask not in ['', None, 'pb']:

            # VLASS-SE-CONT tier-2 masking stage results in two element list for self.mask
            iter_mask = self.mask[iteration-1] if type(self.mask) is list else self.mask

            self._copy_or_regrid_mask(iter_mask, new_cleanmask)

            self.result.cleanmask = new_cleanmask
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter
        else:
            # Special case cleaning without mask if new_cleanmask is 'pb'
            self.result.cleanmask = 'pb' if new_cleanmask == 'pb' else ''
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter

        return self.result

    def _copy_or_regrid_mask(self, usermask, new_cleanmask):
        """Regrid user mask rather than copy if the WCS is mismatched."""

        do_regrid_mask = False
        try:
            extension = '.tt0' if self.multiterm else ''
            template = self.flux+extension
            with casa_tools.ImageReader(template) as image:
                shape_template = image.shape()
            with casa_tools.ImageReader(usermask) as image:
                shape_usermask = image.shape()
            if shape_template[0] != shape_usermask[0] or shape_template[1] != shape_usermask[1]:
                LOG.warning(
                    f'The user-specified mask and the expected imaging products have different shapes ({shape_usermask[0:2]} vs. {shape_template[0:2]}). will regrid the user-supplied mask.')
                do_regrid_mask = True
        except:
            LOG.warning(
                'Failed to get the dimensions of either the user mask or the expected imaging products, still proceed by copying the user-specified mask anyway.')

        if do_regrid_mask:
            imregrid_job = casa_tasks.imregrid(imagename=usermask, template=self.flux+extension,
                                               output=new_cleanmask, overwrite=True, axes=[0, 1], replicate=False, interpolation='nearest')
            if self.__executor is None:
                imregrid_job.execute()
            else:
                self.__executor.execute(imregrid_job)
        else:
            LOG.info('Copying {} to {}'.format(usermask, new_cleanmask))
            copytree_job = casa_tasks.copytree(usermask, new_cleanmask)
            if self.__executor is None:
                copytree_job.execute()
            else:
                self.__executor.execute(copytree_job)

        return
