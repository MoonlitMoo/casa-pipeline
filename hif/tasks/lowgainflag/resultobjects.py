import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.h.tasks.common import flaggableviewresults
from pipeline.infrastructure.refantflag import FullyFlaggedAntennasNotification
from typing import List, Set

LOG = infrastructure.get_logger(__name__)


class LowgainflagResults(basetask.Results,
                         flaggableviewresults.FlaggableViewResults):
    def __init__(self, vis=None):
        """
        Construct and return a new LowgainflagResults.
        """
        basetask.Results.__init__(self)
        flaggableviewresults.FlaggableViewResults.__init__(self)
        self.vis = vis

        # Set of antennas that should be moved to the end of the refant list.
        self.refants_to_demote: Set[str] = set()

        # Set of entirely flagged antennas that should be removed from refants.
        self.refants_to_remove: Set[str] = set()

        # further information about entirely flagged antennas used in QA scoring
        self.fully_flagged_antenna_notifications: List[FullyFlaggedAntennasNotification] = []

    def merge_with_context(self, context):
        # Update reference antennas for MS.
        ms = context.observing_run.get_ms(name=self.vis)
        ms.update_reference_antennas(ants_to_demote=self.refants_to_demote,
                                     ants_to_remove=self.refants_to_remove)

    def __repr__(self):
        s = 'LowgainflagResults'
        return s


class LowgainflagDataResults(basetask.Results):
    def __init__(self):
        """
        Construct and return a new LowgainflagDataResults.
        """
        basetask.Results.__init__(self)

    def merge_with_context(self, context):
        # do nothing, none of the gain cals used for the flagging
        # views should be used elsewhere
        pass

    def __repr__(self):
        s = 'LowgainflagDataResults'
        return s


class LowgainflagViewResults(basetask.Results,
                             flaggableviewresults.FlaggableViewResults):
    def __init__(self):
        """
        Construct and return a new LowgainflagViewResults.
        """
        basetask.Results.__init__(self)
        flaggableviewresults.FlaggableViewResults.__init__(self)

    def merge_with_context(self, context):
        pass

    def __repr__(self):
        s = 'LowgainflagViewResults'
        return s
