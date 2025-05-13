import collections
import copy
import os
from typing import Callable, Dict, Optional

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.callibrary import IntervalCalState

from ...heuristics.fieldnames import IntentFieldnames

__all__ = [
    'Applycal',
    'SerialApplycal',
    'ApplycalInputs',
    'ApplycalResults',
]

LOG = infrastructure.get_logger(__name__)


class ApplycalInputs(vdp.StandardInputs):
    """
    ApplycalInputs defines the inputs for the Applycal pipeline task.
    """
    # PIPE-1691: hif_applycal is now implicitly a parallel task, but by default
    # running with parallel=False.
    parallel = sessionutils.parallel_inputs_impl(default=False)

    @vdp.VisDependentProperty
    def antenna(self):
        return ''

    @antenna.convert
    def antenna(self, value):
        antennas = self.ms.get_antenna(value)
        # if all antennas are selected, return ''
        if len(antennas) == len(self.ms.antennas):
            return ''
        return utils.find_ranges([a.id for a in antennas])

    applymode = vdp.VisDependentProperty(default='calflagstrict')

    @vdp.VisDependentProperty
    def field(self):
        # this will give something like '0542+3243,0343+242'
        field_finder = IntentFieldnames()
        intent_fields = field_finder.calculate(self.ms, self.intent)

        # run the answer through a set, just in case there are duplicates
        fields = set()
        fields.update(utils.safe_split(intent_fields))

        return ','.join(fields)

    flagbackup = vdp.VisDependentProperty(default=True)
    flagdetailedsum = vdp.VisDependentProperty(default=False)
    flagsum = vdp.VisDependentProperty(default=True)
    intent = vdp.VisDependentProperty(default='TARGET,PHASE,BANDPASS,AMPLITUDE,CHECK')
    parang = vdp.VisDependentProperty(default=False)

    @vdp.VisDependentProperty
    def spw(self):
        science_spws = self.ms.get_spectral_windows(with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    def __init__(self, context, output_dir=None, vis=None, field=None, spw=None, antenna=None, intent=None,
                 parang=None, applymode=None, flagbackup=None, flagsum=None, flagdetailedsum=None,
                 parallel=None):
        super().__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.field = field
        self.spw = spw
        self.antenna = antenna
        self.intent = intent

        # solution parameters
        self.parang = parang
        self.applymode = applymode
        self.flagbackup = flagbackup
        self.flagsum = flagsum
        self.flagdetailedsum = flagdetailedsum

        self.parallel = parallel

    def to_casa_args(self):
        casa_args = super().to_casa_args()
        del casa_args['flagsum']
        del casa_args['flagdetailedsum']
        del casa_args['parallel']
        return casa_args


class ApplycalResults(basetask.Results):
    """
    ApplycalResults is the results class for the pipeline Applycal task.
    """

    def __init__(self, applied=None, callib_map: Dict[str, str]=None,
                 data_type: Optional[DataType]=None):
        """
        Construct and return a new ApplycalResults.

        The resulting object should be initialized with a list of
        CalibrationTables corresponding to the caltables applied by this task.

        :param applied: caltables applied by this task
        :type applied: list of :class:`~pipeline.domain.caltable.CalibrationTable`
        """
        if applied is None:
            applied = []
        if callib_map is None:
            callib_map = {}

        super().__init__()
        self.applied = set()
        self.applied.update(applied)
        self.callib_map = dict(callib_map)
        self.data_type = data_type

    def merge_with_context(self, context):
        """
        Merges these results with the given context by examining the context
        and marking any applied caltables, so removing them from subsequent
        on-the-fly calibration calculations.

        See :method:`~pipeline.Results.merge_with_context`
        """
        if not self.applied:
            LOG.error('No results to merge')

        for calapp in self.applied:
            LOG.trace('Marking %s as applied' % calapp.as_applycal())
            context.callibrary.mark_as_applied(calapp.calto, calapp.calfrom)

        # Update data_column
        if self.data_type is not None:
            msobj = context.observing_run.get_ms(self.inputs['vis'])
            colname = 'CORRECTED_DATA'
            # Temporal workaround: restoredata merges context twice
            if msobj.get_data_column(self.data_type) != colname:
                msobj.set_data_column(self.data_type, colname)

    def __repr__(self):
        s = 'ApplycalResults:\n'
        for caltable in self.applied:
            if isinstance(caltable.gaintable, list):
                basenames = [os.path.basename(x) for x in caltable.gaintable]
                s += '\t{name} applied to {vis} spw #{spw}\n'.format(
                    spw=caltable.spw, vis=os.path.basename(caltable.vis),
                    name=','.join(basenames))
            else:
                s += '\t{name} applied to {vis} spw #{spw}\n'.format(
                    name=caltable.gaintable, spw=caltable.spw,
                    vis=os.path.basename(caltable.vis))
        return s


class SerialApplycal(basetask.StandardTaskTemplate):
    """
    Applycal executes CASA applycal tasks for the current active context
    state, applying calibrations registered with the pipeline context to the
    target measurement set.

    Applying the results from this task to the context marks the referred
    tables as applied. As a result, they will not be included in future
    on-the-fly calibration arguments.
    """
    Inputs = ApplycalInputs
    # DataType to be set for a new column
    applied_data_type = DataType.REGCAL_CONTLINE_ALL

    def __init__(self, inputs):
        super().__init__(inputs)

    def modify_task_args(self, task_args):
        task_args['antenna'] = '*&*'
        return task_args

    def _get_flagsum_arg(self, args):
        return args

    def _tweak_flagkwargs(self, template):
        return template

    def prepare(self):
        inputs = self.inputs

        # Get the target data selection for this task as a CalTo object
        calto = callibrary.get_calto_from_inputs(inputs)

        # Now get the calibration state for that CalTo data selection. The
        # returned dictionary of CalTo:CalFroms specifies the calibrations to
        # be applied and the data selection to apply them to. While the CASA
        # callibrary could use the full, untrimmed calibration state for the
        # whole MS, we need the trimmed version for 1) the web log, where it
        # is used to state what specific calibrations were applied, and 2),
        # the pipeline callibrary, where we mark the target data selection as
        # having calibration applied.
        #
        # Note that no 'ignore' argument is given to get_calstate
        # (specifically, we don't say ignore=['calwt'] like many other tasks)
        # as applycal is a task that can handle calwt and so different values
        # of calwt should in this case result in different tasks.
        calstate = inputs.context.callibrary.get_calstate(calto)
        calstate = callibrary.fix_cycle0_data_selection(self.inputs.context, calstate)

        # The 'hide_empty=True' is important here. The working calstate
        # contains empty state for MSes outside the scope of this task, i.e.,
        # MSes in the context that are not specified in inputs.vis for this
        # task. These extra MSes cause problems in weblog code downstream,
        # which expects results.applied to refer to just the MSes specified in
        # the inputs. Using hide_empty gives the data the expected shape.
        merged = calstate.merged(hide_empty=True)

        if os.getenv('DISABLE_CASA_CALLIBRARY', False):
            LOG.info('CASA callibrary disabled: reverting to non-callibrary applycal call')
            jobs = jobs_without_calapply(merged, inputs, self.modify_task_args)
        elif contains_uvcont_table(merged):
            LOG.info('Calibration state contains uvcont tables: reverting to non-callibrary applycal call')
            jobs = jobs_without_calapply(merged, inputs, self.modify_task_args)
        else:
            LOG.info('No uvcont tables in calibration state: using CASA callibrary applycal.')
            jobs = jobs_with_calapply(calstate, inputs, self.modify_task_args)

        # if requested, schedule additional flagging tasks to determine
        # statistics
        if inputs.flagsum:
            summary_args = dict(vis=inputs.vis, mode='summary')
            # give subclasses a chance to tweak flag summary arguments
            summary_args = self._get_flagsum_arg(summary_args)
            # schedule a flagdata summary jobs either side of the applycal jobs
            jobs.insert(0, casa_tasks.flagdata(name='before', **summary_args))
            jobs.append(casa_tasks.flagdata(name='applycal', **summary_args))

            if inputs.flagdetailedsum:
                ms = inputs.context.observing_run.get_ms(inputs.vis)
                # Schedule a flagdata job to determine flagging stats per spw
                # and per field
                flagkwargs = ["spw='{!s}' fieldcnt=True mode='summary' name='AntSpw{:0>3}'".format(spw.id, spw.id)
                              for spw in ms.get_spectral_windows()]

                # give subclasses a change to tweak flag arguments
                flagkwargs = self._tweak_flagkwargs(flagkwargs)

                jobs.append(casa_tasks.flagdata(vis=inputs.vis, mode='list', inpfile=flagkwargs, flagbackup=False))

        # execute the jobs and capture the output
        job_results = [self._executor.execute(job) for job in jobs]
        flagdata_results = [job_result for job, job_result in zip(jobs, job_results) if job.fn_name == 'flagdata']

        applied_calapps = [callibrary.CalApplication(calto, calfroms) for calto, calfroms in merged.items()]

        # give a dict like {'abc123.ms': 'path/to/callibrary'}. The use of
        # dict assumes that there is only one jobrequest per MS, which is true
        # when the CASA callibrary is used.
        vis_to_callib = {job.kw['vis']: job.kw['callib'] for job in jobs
                         if job.fn_name == 'applycal' and 'callib' in job.kw}

        result = ApplycalResults(applied_calapps, callib_map=vis_to_callib,
                                 data_type=self.applied_data_type)

        # add and reshape the flagdata results if required
        if inputs.flagsum:
            result.summaries = [flagdata_results[0], flagdata_results[1]]
            if inputs.flagdetailedsum:
                reshaped_flagsummary = reshape_flagdata_summary(flagdata_results[2])
                processed_flagsummary = self.process_flagsummary(reshaped_flagsummary)
                result.flagsummary = processed_flagsummary

        return result

    def analyse(self, result):
        return result

    def process_flagsummary(self, flagsummary):
        """
        Template entry point for processing flagdata summary dicts. Override
        this function to filter or otherwise process the flagdata summary
        results.

        :param flagsummary: the unfiltered, unprocessed flagsummary dict
        :return:
        """
        return flagsummary


def reshape_flagdata_summary(flagdata_result):
    """
    Reshape a flagdata result so that results are grouped by field.

    :param flagdata_result:
    :return:
    """
    # Set into single dictionary report (single spw) if only one dict returned
    if not all([key.startswith('report') for key in flagdata_result]):
        flagdata_result = {'report0': flagdata_result}

    flagsummary = collections.defaultdict(dict)
    for report_level, report in flagdata_result.items():
        report_name = report['name']
        report_type = report['type']
        # report keys are all fieldnames with the exception of 'name' and
        # 'type', which are in there too.
        for field_name in [key for key in report if key not in ('name', 'type')]:
            # deepcopy to avoid modifying the results dict
            flagsummary[field_name][report_level] = copy.deepcopy(report[field_name])
            flagsummary[field_name][report_level]['name'] = '{!s}Field_{!s}'.format(report_name, field_name)
            flagsummary[field_name][report_level]['type'] = report_type

    return flagsummary


# def limit_fields(flagsummary, ms):
#     calibrator_fields = ms.get_fields(intent='AMPLITUDE,PHASE,BANDPASS')
#
#     target_fields = ms.get_fields(intent='TARGET')
#     plot_stride = len(target_fields) / 30 + 1
#     targetfields = target_fields[::plot_stride]
#
#     fields_to_plot = calibrator_fields + targetfields
#
#     return {k: v for k, v in flagsummary.items() if k in fields_to_plot}


@task_registry.set_equivalent_casa_task('h_applycal')
@task_registry.set_casa_commands_comment('Calibrations are applied to the data. Final flagging summaries are computed')
class Applycal(sessionutils.ParallelTemplate):
    Inputs = ApplycalInputs
    Task = SerialApplycal


def jobs_without_calapply(merged, inputs, mod_fn):
    jobs = []
    # sort for a stable applycal order, to make diffs easier to parse
    for calto, calfroms in sorted(merged.items()):
        # if there's nothing to apply for this data selection, continue. This
        # should never be seen as merged is called with hide_empty=True
        if not calfroms:
            LOG.info('There is no calibration information for field %s intent %s spw %s in %s' %
                     (str(calto.field), str(calto.intent), str(calto.spw), inputs.ms.basename))
            continue

        # arrange a calibration job for the unique data selection
        inputs.spw = calto.spw
        inputs.field = calto.field
        inputs.intent = calto.intent

        task_args = inputs.to_casa_args()

        # set the on-the-fly calibration state for the data selection.
        calapp = callibrary.CalApplication(calto, calfroms)
        task_args['gaintable'] = calapp.gaintable
        task_args['gainfield'] = calapp.gainfield
        task_args['spwmap'] = calapp.spwmap
        task_args['interp'] = calapp.interp
        task_args['calwt'] = calapp.calwt
        task_args['applymode'] = inputs.applymode

        # give subclasses a chance to modify the task arguments
        task_args = mod_fn(task_args)

        jobs.append(casa_tasks.applycal(**task_args))

    return jobs


def jobs_with_calapply(calstate: IntervalCalState, inputs: ApplycalInputs, mod_fn: Callable):
    callibrary_file = '{}.s{}.{}.callibrary'.format(inputs.vis,
                                                    inputs.context.task_counter,
                                                    inputs.context.subtask_counter)
    ms = inputs.context.observing_run.get_ms(inputs.vis)
    calstate.export_to_casa_callibrary(ms, callibrary_file)

    # No callibrary file will be created when the merged calstate does not
    # require the application of calibrations.
    if not os.path.exists(callibrary_file):
        LOG.info('No applycal job required for CASA callibrary: {}'.format(callibrary_file))
        return []

    calstate_file = '{}.s{}.{}.calstate'.format(inputs.vis,
                                                inputs.context.task_counter,
                                                inputs.context.subtask_counter)
    with open(calstate_file, "w") as applyfile:
        applyfile.write('# Apply file for %s\n' % (os.path.basename(inputs.vis)))
        applyfile.write(calstate.as_applycal())

    task_args = inputs.to_casa_args()

    # Don't delete spw, field, or intents as the inputs may request
    # calibration of a subset of the total MS. The CASA callibrary can
    # still define calibration for the whole MS, that's not a problem.
    for a in ['gaintable', 'gainfield', 'spwmap', 'interp', 'calwt']:
        if a in task_args:
            del task_args[a]

    task_args['applymode'] = inputs.applymode
    task_args['docallib'] = True
    task_args['callib'] = callibrary_file

    mod_fn(task_args)

    return [casa_tasks.applycal(**task_args)]


def contains_uvcont_table(merged):
    return 'uvcont' in [calfrom.caltype for calfroms in merged.values() for calfrom in calfroms]
