import collections

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask

LOG = infrastructure.get_logger(__name__)


class SessionRefAntResults(basetask.Results):

    def __init__(self):
        super(SessionRefAntResults, self).__init__()

        # Initialize dictionary of sessions, mapping each session to list of
        # evaluated MSes and final refant chosen for that session.
        self.refant = collections.defaultdict(dict)

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.api.Results.merge_with_context`
        """
        # For each session, if a best refant was identified, then update the
        # refant lists for the corresponding measurement sets.
        for session_name in self.refant:
            if self.refant[session_name]['refant']:
                # Update refant list for each MS that was evaluated for this
                # session.
                for vis in self.refant[session_name]['vislist']:
                    ms = context.observing_run.get_ms(name=vis)

                    # Set new reference antenna list.
                    ms.reference_antenna = self.refant[session_name]['refant']

    def __repr__(self):
        return 'SessionRefAntResults'
