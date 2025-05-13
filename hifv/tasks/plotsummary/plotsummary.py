import pipeline.h.tasks.applycal as h_applycal
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class PlotSummaryInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


class PlotSummaryResults(h_applycal.ApplycalResults):
    def merge_with_context(self, context):
        """
        Merges these results with the given context by examining the context
        and marking any applied caltables, so removing them from subsequent
        on-the-fly calibration calculations.

        See :method:`~pipeline.Results.merge_with_context`
        """
        if not self.applied:
            LOG.error('No results to merge')

        #for calapp in self.applied:
        #    LOG.trace('Marking %s as applied' % calapp.as_applycal())
        #    context.callibrary.mark_as_applied(calapp.calto, calapp.calfrom)


@task_registry.set_equivalent_casa_task('hifv_plotsummary')
class PlotSummary(basetask.StandardTaskTemplate):
    Inputs = PlotSummaryInputs

    def prepare(self):
        # get the applied calibration state from callibrary.active. This holds
        # the CalApplications for everything applied by the pipeline in this run.
        applied = self.inputs.context.callibrary.applied.merged()

        calapps = [callibrary.CalApplication(calto, calfroms) for calto, calfroms in applied.items()]

        result = PlotSummaryResults(applied=calapps)

        return result

    def analyse(self, results):
        return results
