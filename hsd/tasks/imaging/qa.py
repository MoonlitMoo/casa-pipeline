import pipeline.infrastructure.logging as logging
import pipeline.qa.scorecalculator as qacalc
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.h.tasks.exportdata.aqua as aqua
from . import imaging
from . import resultobjects

LOG = logging.get_logger(__name__)


class SDImagingQAHandler(pqa.QAPlugin):
    """
    SDImagingQAHandler is qa handler for each image product represented
    as the SDImagingResultItem instance.
    """
    result_cls = resultobjects.SDImagingResultItem
    child_cls = None
    generating_task = imaging.SDImaging

    def handle(self, context, result):
        """
        This handles single SDImagingResultItem.
        """
        # result.outcome should have 'image'
        if 'image' not in result.outcome:
            return

        # we only evaluate the score for combined image
        antenna_name = result.outcome['image'].antenna
        if antenna_name != 'COMBINED':
            return

        score_masked = qacalc.score_sdimage_masked_pixels(context, result)
        result.qa.pool.append(score_masked)

        score_contamination = qacalc.score_sdimage_contamination(context, result)
        result.qa.pool.append(score_contamination)
        
        score_resterscan_raster_gap = qacalc.score_rasterscan_correctness_imaging_raster_gap(result)
        result.qa.pool.extend(score_resterscan_raster_gap)

        score_resterscan_incomplete = qacalc.score_rasterscan_correctness_imaging_raster_analysis_incomplete(result)
        result.qa.pool.extend(score_resterscan_incomplete)


class SDImagingListQAHandler(pqa.QAPlugin):
    """
    SDImagingListQAHandler is qa handler for a list of image products
    represented as the SDImagingResults. SDImagingResults is a subclass
    of ResultsList and contains SDImagingResultsItem instances.
    """
    result_cls = resultobjects.SDImagingResults
    child_cls = resultobjects.SDImagingResultItem

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated

aqua_exporter = aqua.xml_generator_for_metric('SingleDishImageMaskedPixels', '{:0.3}')
aqua.register_aqua_metric(aqua_exporter)
