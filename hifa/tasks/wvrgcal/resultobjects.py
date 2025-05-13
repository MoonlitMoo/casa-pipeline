import collections
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.h.tasks.common import commonresultobjects

LOG = infrastructure.get_logger(__name__)


class WvrgcalResult(basetask.Results):

    def __init__(self, vis, final=None, pool=None, preceding=None, wvrflag=None):
        """
        Construct and return a new WvrgcalResult.
        """
        super(WvrgcalResult, self).__init__()

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []
        if wvrflag is None:
            wvrflag = []

        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()

        # section for qa results
        self.qa_wvr = commonresultobjects.QaResult()

        # results used to calculate the qa results
        self.qa_wvr.bandpass_result = None
        self.qa_wvr.nowvr_result = None
        self.qa_wvr.qa_spw = None

        # views and flag operations
        self.flagging = []
        self.wvrflag = wvrflag
        self.view = collections.defaultdict(list)

        # record wvrgcal tie arguments for weblog 
        self.tie = ''

        # various flags
        self.PHnoisy = False
        self.BPnoisy = False
        self.suggest_remcloud = False

    def merge_with_context(self, context):
        if not self.final:
            LOG.info('No results to merge')
            return

        for calapp in self.final:
            LOG.debug('Adding calibration to callibrary:\n'
                      '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)

        if self.wvrflag:
            ms = context.observing_run.get_ms(name=self.vis)
            if (hasattr(ms, 'reference_antenna')
                    and isinstance(ms.reference_antenna, str)):
                refant = ms.reference_antenna.split(',')

                # PIPE-2057: CASA 'wvrgcal' cannot evaluate ALMA antennas that
                # do not have WVR radiometers (which are the antennas whose name
                # begins with "CM") and will automatically mark those as
                # flagged. Since these "CM" antennas were not evaluated, do not
                # let them be removed from the refant list for having "bad WVR".
                if any("CM" in ant for ant in self.wvrflag):
                    LOG.debug(f"{ms.basename}: wvrgcal has flagged 'CM' antennas, but this happens not because their"
                              f" WVR data are bad, but because these antennas do not have WVR data at all (no WVR"
                              f" radiometers). Since the 'CM' antennas were not evaluated, they are not removed here"
                              f" from the refant list. Any non-'CM' antenna that got flagged by wvrgcal will get"
                              f" removed from the refant list.")
                bad_antennas = {ant for ant in self.wvrflag if "CM" not in ant}.intersection(refant)
                if bad_antennas:
                    ms.update_reference_antennas(ants_to_remove=bad_antennas)

    def __repr__(self):

        # Format the Wvrgcal results.
        s = 'WvrgcalResult:\n'
        if not self.final:
            s += '\tNo wvr caltables will be applied\n'

        for calapplication in self.final:
            s += '\tBest caltable for spw #{spw} field {field} in {vis} is {name}\n'.format(
              spw=calapplication.spw, field=calapplication.field,
              vis=os.path.basename(calapplication.vis),
              name=calapplication.gaintable)
        s += '\twvrflag is {wvrflag}'.format(wvrflag=self.wvrflag)

        return s
