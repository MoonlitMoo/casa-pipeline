import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.imagelibrary as imagelibrary
from .. import common
from ..common import utils as sdutils

LOG = infrastructure.get_logger(__name__)


class SDImagingResultItem(common.SingleDishResults):
    """
    The class to store result of each image.
    """
    def __init__(self, task=None, success=None, outcome=None, sensitivity_info=None,
                 theoretical_rms=None, frequency_channel_reversed=False):
        super(SDImagingResultItem, self).__init__(task, success, outcome)
        self.sensitivity_info = sensitivity_info
        self.theoretical_rms = theoretical_rms
        self.frequency_channel_reversed = frequency_channel_reversed
        # logrecords attribute is mandatory but not created unless Result is returned by execute.
        self.logrecords = []
        # raster scan heuristics results for QAscore calculation
        self.rasterscan_heuristics_results_rgap = {}  # {originms : [RasterScanHeuristicsResult]}
        self.rasterscan_heuristics_results_incomp = {}  # {originms : [RasterScanHeuristicsResult]}

    def merge_with_context(self, context):
        super(SDImagingResultItem, self).merge_with_context(context)
        LOG.todo('need to decide what is done in SDImagingResultItem.merge_with_context')

        # check if data is NRO
        is_nro = sdutils.is_nro(context)

        if 'export_results' in self.outcome:
            self.outcome['export_results'].merge_with_context(context)

        # register ImageItem object to context.sciimlist if antenna is COMBINED
        if 'image' in self.outcome:
            image_item = self.outcome['image']
            if is_nro:
                # NRO requirement is to export per-beam (per-antenna) images
                # as well as combined ones
                cond = isinstance(image_item, imagelibrary.ImageItem)
            else:
                # ALMA requirement is to export only combined images
                cond = isinstance(image_item, imagelibrary.ImageItem) and image_item.antenna == 'COMBINED'
            if cond:
                context.sciimlist.add_item(image_item)

    def _outcome_name(self):
        # return [image.imagename for image in self.outcome]
        return self.outcome['image'].imagename


class SDImagingResults(basetask.ResultsList):
    """
    The class to store a list of per image results (SDImagingResultItem).
    """
    def merge_with_context(self, context):
        # Assign logrecords of top level task to the first result item.
        if hasattr(self, 'logrecords') and len(self) > 0:
            self[0].logrecords.extend(self.logrecords)
        # merge per item
        super(SDImagingResults, self).merge_with_context(context)


class SDImagingWorkerResults(common.SingleDishResults):
    def __init__(self, task=None, success=None, outcome=None):
        super(SDImagingWorkerResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context):
        super(SDImagingWorkerResults, self).merge_with_context(context)

    def _outcome_name(self):
        # return [image.imagename for image in self.outcome]
        return self.outcome
