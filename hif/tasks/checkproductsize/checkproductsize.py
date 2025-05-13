import pipeline.infrastructure as infrastructure
#import pipeline.infrastructure.api as api
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.project as project
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hif.heuristics import checkproductsize
from pipeline.infrastructure import task_registry
from .resultobjects import CheckProductSizeResult

LOG = infrastructure.get_logger(__name__)


class CheckProductSizeInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    parallel = vdp.VisDependentProperty(default='automatic')

    @vdp.VisDependentProperty(null_input=[None, '', -1, -1.0])
    def maxcubelimit(self):
        return project.PerformanceParameters().max_cube_size

    @vdp.VisDependentProperty(null_input=[None, '', -1, -1.0])
    def maxcubesize(self):
        return project.PerformanceParameters().max_cube_size

    @vdp.VisDependentProperty(null_input=[None, '', -1, -1.0])
    def maxproductsize(self):
        return project.PerformanceParameters().max_product_size

    @vdp.VisDependentProperty(null_input=[None, '', -1, -1.0])
    def maximsize(self):
        return -1

    def __init__(self, context, output_dir=None, vis=None, maxcubesize=None, maxcubelimit=None, maxproductsize=None,
                 calcsb=None, parallel=None, maximsize=None):
        super(CheckProductSizeInputs, self).__init__()

        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.maxcubesize = maxcubesize
        self.maxcubelimit = maxcubelimit
        self.maxproductsize = maxproductsize
        self.maximsize = maximsize
        self.calcsb = calcsb
        self.parallel = parallel


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
#api.ImagingMeasurementSetsPreferred.register(CheckProductSizeInputs)


@task_registry.set_equivalent_casa_task('hif_checkproductsize')
class CheckProductSize(basetask.StandardTaskTemplate):
    Inputs = CheckProductSizeInputs

    is_multi_vis_task = True

    def prepare(self):

        # Check parameter settings

        # Initialize skip_status_msgs to None. If the condition for triggering heuristics isn't met,
        # assign a 3-element tuple: (status, longmsg, shortmsg).
        skip_status_msgs = None

        # Check if no size limits are given.
        if self.inputs.maxcubesize == -1 and self.inputs.maxcubelimit == -1 and self.inputs.maxproductsize == -1 and self.inputs.maximsize == -1:
            LOG.info('No size limits given.')
            skip_status_msgs = ('OK', 'No size limits given', 'No size limits')

        # Mitigate either product byte size or image pixel count, but not both.
        elif (self.inputs.maxcubesize != -1 or self.inputs.maxcubelimit != -1 or self.inputs.maxproductsize != -1) and self.inputs.maximsize != -1:
            skip_status_msgs = (
                'ERROR', 'Parameter error: cannot mitigate product byte size and image pixel count at the same time.', 'Parameter error')

        # Check for parameter errors: maxcubelimit must be >= maxcubesize.
        elif self.inputs.maxcubesize != -1 and self.inputs.maxcubelimit != -1 and self.inputs.maxcubesize > self.inputs.maxcubelimit:
            skip_status_msgs = ('ERROR', 'Parameter error: maxcubelimit must be >= maxcubesize', 'Parameter error')

        # Check for parameter errors: maxproductsize must be > maxcubesize.
        elif self.inputs.maxcubesize != -1 and self.inputs.maxproductsize != -1 and self.inputs.maxcubesize >= self.inputs.maxproductsize:
            skip_status_msgs = ('ERROR', 'Parameter error: maxproductsize must be > maxcubesize', 'Parameter error')

        # Check for parameter errors: maxproductsize must be > maxcubelimit.
        elif self.inputs.maxcubelimit != -1 and self.inputs.maxproductsize != -1 and self.inputs.maxcubelimit >= self.inputs.maxproductsize:
            skip_status_msgs = ('ERROR', 'Parameter error: maxproductsize must be > maxcubelimit', 'Parameter error')

        # Skip the VLA cube production size mitigation check-up if no CONTLINE_SCIENCE or LINE_SCIENCE datatype
        # is registered in the Pipeline context.
        elif self.inputs.maximsize == -1 and self.inputs.context.vla_skip_mfs_and_cube_imaging:
            skip_status_msgs = (
                'OK', 'Skip the VLA cube product size mitigation due to absence of required datatypes: CONTLINE_SCIENCE or LINE_SCIENCE',
                'Stage skipped')

        # If skip_status_msgs is set, create a CheckProductSizeResult object and log the summary information.
        if skip_status_msgs:
            result = CheckProductSizeResult(self.inputs.maxcubesize,
                                            self.inputs.maxcubelimit,
                                            self.inputs.maxproductsize,
                                            -1,
                                            -1,
                                            -1,
                                            -1,
                                            -1,
                                            self.inputs.maximsize,
                                            -1,
                                            -1,
                                            {},
                                            skip_status_msgs[0],
                                            {'longmsg': skip_status_msgs[1], 'shortmsg': skip_status_msgs[2]},
                                            None)
            # Log summary information
            LOG.info('%s', result)
            return result

        checkproductsize_heuristics = checkproductsize.CheckProductSizeHeuristics(self.inputs)

        # Clear any previous size mitigation parameters
        self.inputs.context.size_mitigation_parameters = {}

        
        if self.inputs.maximsize != -1:
            # Mitigate image pixel count (used for VLA, see PIPE-676)
            size_mitigation_parameters, \
            original_maxcubesize, original_productsize, \
            cube_mitigated_productsize, \
            maxcubesize, productsize, \
            original_imsize, mitigated_imsize, \
            error, reason, \
            known_synthesized_beams = \
                checkproductsize_heuristics.mitigate_imsize()
        else:
            # Mitigate data product byte size (used for ALMA and VLA, see PIPE-2231)
            size_mitigation_parameters, \
            original_maxcubesize, original_productsize, \
            cube_mitigated_productsize, \
            maxcubesize, productsize, \
            original_imsize, mitigated_imsize, \
            error, reason, \
            known_synthesized_beams = \
                checkproductsize_heuristics.mitigate_sizes()

        if error:
            status = 'ERROR'
        elif size_mitigation_parameters != {}:
            status = 'MITIGATED'
        else:
            status = 'OK'

        size_mitigation_parameters['status'] = status

        result = CheckProductSizeResult(self.inputs.maxcubesize,
                                        self.inputs.maxcubelimit,
                                        self.inputs.maxproductsize,
                                        original_maxcubesize,
                                        original_productsize,
                                        cube_mitigated_productsize,
                                        maxcubesize,
                                        productsize,
                                        self.inputs.maximsize,
                                        original_imsize,
                                        mitigated_imsize,
                                        size_mitigation_parameters,
                                        status,
                                        reason,
                                        known_synthesized_beams)

        # Log summary information
        LOG.info(str(result))

        return result

    def analyse(self, result):
        return result
