import copy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.basetask as basetask

LOG = infrastructure.get_logger(__name__)


class CheckProductSizeResult(basetask.Results):
    def __init__(self,
                 allowed_maxcubesize,
                 allowed_maxcubelimit,
                 allowed_productsize,
                 original_maxcubesize,
                 original_productsize,
                 cube_mitigated_productsize,
                 mitigated_maxcubesize,
                 mitigated_productsize,
                 allowed_maximsize,
                 original_maximsize,
                 mitigated_maximsize,
                 size_mitigation_parameters,
                 status,
                 reason,
                 synthesized_beams):
        super().__init__()
        self.allowed_maxcubesize = allowed_maxcubesize
        self.allowed_maxcubelimit = allowed_maxcubelimit
        self.allowed_productsize = allowed_productsize
        self.original_maxcubesize = original_maxcubesize
        self.original_productsize = original_productsize
        self.cube_mitigated_productsize = cube_mitigated_productsize
        self.mitigated_maxcubesize = mitigated_maxcubesize
        self.mitigated_productsize = mitigated_productsize
        self.allowed_maximsize = allowed_maximsize
        self.original_maximsize = original_maximsize
        self.mitigated_maximsize = mitigated_maximsize
        self.size_mitigation_parameters = size_mitigation_parameters
        self.status = status
        self.reason = reason
        self.synthesized_beams = synthesized_beams

    def merge_with_context(self, context):
        # Store mitigation parameters for subsequent hif_makeimlist calls.
        context.size_mitigation_parameters = self.size_mitigation_parameters

        # Calculated beams for later stages
        if self.synthesized_beams is not None:
            if 'recalc' in self.synthesized_beams:
                context.synthesized_beams = copy.deepcopy(self.synthesized_beams)
                del context.synthesized_beams['recalc']
            else:
                utils.update_beams_dict(context.synthesized_beams, self.synthesized_beams)

    def __repr__(self):
        repr = 'CheckProductSize:\n'
        repr += ' Status: %s\n' % (self.status)
        repr += ' Reason: %s\n' % (self.reason['longmsg'])
        repr += ' Allowed maximum cube size: %.3g GB\n' % (self.allowed_maxcubesize)
        repr += ' Allowed maximum cube limit: %.3g GB\n' % (self.allowed_maxcubelimit)
        repr += ' Predicted maximum cube size: %.3g GB\n' % (self.original_maxcubesize)
        repr += ' Mitigated maximum cube size: %.3g GB\n' % (self.mitigated_maxcubesize)
        repr += ' Allowed maximum product size: %.3g GB\n' % (self.allowed_productsize)
        repr += ' Initial predicted product size: %.3g GB\n' % (self.original_productsize)
        repr += ' Predicted product size after cube size mitigation: %.3g GB\n' % (self.cube_mitigated_productsize)
        repr += ' Mitigated product size: %.3g GB\n' % (self.mitigated_productsize)
        # PIPE-676: mitigate imsize
        repr += ' Allowed maximum image pixel count: %s\n' % (self.allowed_maximsize)
        repr += ' Predicted image pixel count: %s\n' % (self.original_maximsize)
        repr += ' Mitigated image pixel count: %s\n' % (self.mitigated_maximsize)
        repr += ' Mitigation parameters:\n'
        for parameter, value in self.size_mitigation_parameters.items():
            repr += '  %s: %s\n' % (parameter, value)

        return repr
