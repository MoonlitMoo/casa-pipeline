import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


class ImagePreCheckHeuristics(object):
    def __init__(self, inputs):
        self.inputs = inputs
        self.context = inputs.context

    # Below maxBR is the maxAllowedBeamAxialRatio that will be in the SBSummary table as of cycle 7
    # Note that axial ratio comparisons have been disabled for Cycle 7 (see PIPE-208).
    # We still calculate and post ratios in the weblog.
    def compare_beams(self, beam_0p0, beam_0p5, beam_1p0, beam_2p0, minAR, maxAR, maxBR):

        cqa = casa_tools.quanta

        beams = {0.0: beam_0p0, 0.5: beam_0p5, 1.0: beam_1p0, 2.0: beam_2p0}

        # Define predicted beam areas and beam ratios.
        if beam_0p0 is not None:
            beamArea_0p0 = cqa.mul(beam_0p0['minor'], beam_0p0['major'])
            # Ratios should be rounded to 2 digits (PIPE-208)
            beamRatio_0p0 = cqa.tos(cqa.div(beam_0p0['major'], beam_0p0['minor']), 2)
        else:
            beamArea_0p0 = None
            beamRatio_0p0 = None

        if beam_0p5 is not None:
            beamArea_0p5 = cqa.mul(beam_0p5['minor'], beam_0p5['major'])
            beamRatio_0p5 = cqa.tos(cqa.div(beam_0p5['major'], beam_0p5['minor']), 2)
        else:
            beamArea_0p5 = None
            beamRatio_0p5 = None

        if beam_1p0 is not None:
            beamArea_1p0 = cqa.mul(beam_1p0['minor'], beam_1p0['major'])
            beamRatio_1p0 = cqa.tos(cqa.div(beam_1p0['major'], beam_1p0['minor']), 2)
        else:
            beamArea_1p0 = None
            beamRatio_1p0 = None

        if beam_1p0 is not None:
            beamArea_2p0 = cqa.mul(beam_2p0['minor'], beam_2p0['major'])
            beamRatio_2p0 = cqa.tos(cqa.div(beam_2p0['major'], beam_2p0['minor']), 2)
        else:
            beamArea_2p0 = None
            beamRatio_2p0 = None

        # Define PI requested beam area range
        minARbeamArea = cqa.mul(minAR, minAR)
        maxARbeamArea = cqa.mul(maxAR, maxAR)

        # Define a default value of maxBR if none is available (i.e. Cycle 5, 6 data)
        if cqa.getvalue(maxBR) == 0.0:
            maxBR = cqa.quantity(2.5)

        # PI requested resolution range is not available, robust=0.5 (pre-Cycle 5 data and all 7m-array datasets)
        if (cqa.getvalue(minAR) == 0.0) and \
           (cqa.getvalue(maxAR) == 0.0):
            hm_robust = 0.5
            hm_robust_score_value = 1.0
            hm_robust_score_longmsg = 'No beam goal information found'
            hm_robust_score_shortmsg = 'No beam goal'

        # robust=0.5 beam area in range
        elif cqa.le(minARbeamArea, beamArea_0p5) and \
             cqa.le(beamArea_0p5, maxARbeamArea):
            hm_robust = 0.5
            hm_robust_score_value = 1.0
            hm_robust_score_longmsg = 'Predicted robust=0.5 beam is within the PI requested range'
            hm_robust_score_shortmsg = 'Beam within range'

        # robust=0.0 beam area in range
        elif beamArea_0p0 is not None and \
             cqa.le(minARbeamArea, beamArea_0p0) and \
             cqa.le(beamArea_0p0, maxARbeamArea):
            hm_robust = 0.0
            hm_robust_score_value = 0.85
            hm_robust_score_longmsg = 'Predicted non-default robust=0.0 beam is within the PI requested range'
            hm_robust_score_shortmsg = 'Beam within range using non-default robust'

        # robust=1.0 beam area in range
        elif beamArea_1p0 is not None and \
             cqa.le(minARbeamArea, beamArea_1p0) and \
             cqa.le(beamArea_1p0, maxARbeamArea):
            hm_robust = 1.0
            hm_robust_score_value = 0.85
            hm_robust_score_longmsg = 'Predicted non-default robust=1.0 beam is within the PI requested range'
            hm_robust_score_shortmsg = 'Beam within range using non-default robust'

        # robust=2.0 beam area in range
        elif beamArea_2p0 is not None and \
             cqa.le(minARbeamArea, beamArea_2p0) and \
             cqa.le(beamArea_2p0, maxARbeamArea):
            hm_robust = 2.0
            hm_robust_score_value = 0.85
            hm_robust_score_longmsg = 'Predicted non-default robust=2.0 beam is within the PI requested range'
            hm_robust_score_shortmsg = 'Beam within range using non-default robust'

        # robust=2.0 beam area out of range
        elif beamArea_2p0 is not None and \
             cqa.lt(beamArea_2p0, minARbeamArea):
            hm_robust = 2.0
            hm_robust_score_value = 0.25
            hm_robust_score_longmsg = 'The beam is too small, the predicted non-default robust=2.0 beam cannot achieve PI beam area'
            hm_robust_score_shortmsg = 'Beam is too small'
        # robust=0.0 beam area out of range
        elif beamArea_0p0 is not None and \
             cqa.gt(beamArea_0p0, maxARbeamArea):
            hm_robust = 0.0
            hm_robust_score_value = 0.25
            hm_robust_score_longmsg = 'The beam is too large, the predicted non-default robust=0.0 beam cannot achieve PI beam area'
            hm_robust_score_shortmsg = 'Beam is too large'
        else:
            hm_robust = 0.5
            hm_robust_score_value = 0.25
            hm_robust_score_longmsg = 'Requested beam area range falls in robust gap'
            hm_robust_score_shortmsg = 'Requested beam falls in robust gap'

        return hm_robust, (hm_robust_score_value, hm_robust_score_longmsg, hm_robust_score_shortmsg), beamRatio_0p0, beamRatio_0p5, beamRatio_1p0, beamRatio_2p0
