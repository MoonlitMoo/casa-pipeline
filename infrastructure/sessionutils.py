import abc
import collections
import datetime
import itertools
import os
import tempfile
import traceback
from inspect import signature

from pipeline.infrastructure import basetask
from pipeline.infrastructure import exceptions
from pipeline.infrastructure import logging
from . import mpihelpers
from . import utils
from . import vdp

__all__ = [
    'as_list',
    'group_into_sessions',
    'parallel_inputs_impl',
    'ParallelTemplate',
    'remap_spw_int',
    'remap_spw_str',
    'VDPTaskFactory',
    'VisResultTuple'
]

LOG = logging.get_logger(__name__)

# VisResultTuple is a data structure used by VDPTaskFactor to group
# inputs and results.
VisResultTuple = collections.namedtuple('VisResultTuple', 'vis inputs result')


def parallel_inputs_impl(default='automatic'):
    """
    Get a vis-independent property implementation for a parallel
    Inputs argument.

    :return: Inputs property implementation
    :rtype: property
    """

    def fget(self):
        return self._parallel

    def fset(self, value):
        if value is None:
            value = default
        else:
            allowed = ('true', 'false', 'automatic', True, False)
            if value not in allowed:
                m = ', '.join(('{!r}'.format(i) for i in allowed))
                raise ValueError('Value not in allowed value set ({!s}): {!r}'.format(m, value))
        self._parallel = value

    return property(fget, fset)


def as_list(o):
    return o if isinstance(o, list) else [o]


def group_into_sessions(context, all_results, measurement_sets=None):
    """
    Return results grouped into lists by session.

    Sessions and results are sorted chronologically.

    In terms of the returned dictionary, it means that keys and
    each list associated with the key are all sorted chronologically.

    :param context: pipeline context
    :type context: Context
    :param all_results: result to be grouped
    :type all_results: list
    :param measurement_sets: additional measurementset list (optional)
    :type measurement_sets: list
    :return: dict of sessions to results for that session
    :rtype: dict {session name: [result, result, ...]
    """
    ms_set = set(context.observing_run.measurement_sets)

    if measurement_sets:
        ms_set.update(measurement_sets)

    session_map = {ms.basename: ms.session for ms in ms_set}

    ms_start_times = {ms.basename: utils.get_epoch_as_datetime(ms.start_time)
                      for ms in ms_set}

    def get_session(r):
        basename = os.path.basename(r[0])
        return session_map.get(basename, 'Shared')

    def get_start_time(r):
        basename = os.path.basename(r[0])
        return ms_start_times.get(basename, datetime.datetime.utcfromtimestamp(0))

    def chrono_sort_results(arg_tuple):
        session_id, results = arg_tuple
        return session_id, sorted(results, key=get_start_time)

    def get_session_start_time(arg_tuple):
        # precondition: results are sorted within session in advance
        session_id, results = arg_tuple
        # start time of the session is start time of the first MS in the session
        return get_start_time(results[0])

    # group results by session, and sort results chronologically within session
    results_grouped_by_session = map(
        chrono_sort_results,
        itertools.groupby(sorted(all_results, key=get_session), key=get_session)
    )

    # sort session chronologically and generate ordered dictionary
    return dict(sorted(results_grouped_by_session, key=get_session_start_time))


def group_vislist_into_sessions(context, vislist):
    """
    Group the specified list of measurement sets 'vislist' by their session,
    and return a dictionary that maps a session name to the corresponding list
    of measurement sets.

    :param context: pipeline context
    :type context: :class:`~pipeline.infrastructure.launcher.Context`
    :param vislist: list of vis to be grouped
    :type vislist: list
    :return: dictionary of sessions to vislist for that session
    :rtype: dict {session name: [vis, vis, ...]}
    """
    sessions = collections.defaultdict(list)
    for vis in vislist:
        sessions[getattr(context.observing_run.get_ms(vis), 'session', 'Shared')].append(vis)
    return sessions


def get_vislist_for_session(context, session):
    """
    Return list of measurement sets for specified session name.

    :param context: pipeline context
    :type context: :class:`~pipeline.infrastructure.launcher.Context`
    :param session: name of session for which to retrieve list of vis
    :type session: str
    :return: list of vis for session
    :rtype: list [vis, vis, ...]
    """
    return [ms.name for ms in context.observing_run.get_measurement_sets() if ms.session == session]


class VDPTaskFactory(object):
    """
    VDPTaskFactory is a class that implements the Factory design
    pattern, returning tasks that execute on an MPI client or locally
    as appropriate.

    The correctness of this task is dependent on the correct mapping of
    Inputs arguments to measurement set, hence it is dependent on
    Inputs objects that sub-class VDP StandardInputs.
    """

    def __init__(self, inputs, executor, task):
        """
        Create a new VDPTaskFactory.

        :param inputs: inputs for the task
        :type inputs: class that extends vdp.StandardInputs
        :param executor: pipeline task executor
        :type executor: basetask.Executor
        :param task: task to execute
        """
        self.__inputs = inputs
        self.__context = inputs.context
        self.__executor = executor
        self.__task = task
        self.__context_path = None

    def __enter__(self):
        # If there's a possibility that we'll submit MPI jobs, save the context
        # to disk ready for import by the MPI servers.
        if mpihelpers.mpiclient:
            # Use the tempfile module to generate a unique temporary filename,
            # which we use as the output path for our pickled context
            tmpfile = tempfile.NamedTemporaryFile(suffix='.context',
                                                  dir=self.__context.output_dir,
                                                  delete=True)
            self.__context_path = tmpfile.name
            tmpfile.close()

            self.__context.save(self.__context_path)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__context_path and os.path.exists(self.__context_path):
            os.unlink(self.__context_path)

    def get_task(self, vis):
        """
        Create and return a SyncTask or AsyncTask for the job.

        :param vis: measurement set to create a job for
        :type vis: str
        :return: task object ready for execution
        :rtype: a tuple of (task arguments, (SyncTask|AsyncTask)
        """
        # get the task arguments for the targetted MS
        task_args = self.__get_task_args(vis)

        # Validate the task arguments against the signature expected by
        # the task. This is necessary as self.__inputs could be a
        # different type from the Inputs instance associated with the
        # task, and hence and hence have a different signature. For
        # instance, the session-aware Inputs classes accept a
        # 'parallel' argument, which the non-session tasks do not.
        #
        # To make things complicated, ModeInputs are a special case.
        # Here we shouldn't inspect the constructor signature of the
        # task as it doesn't reflect that of the active task, which is
        # the one that we would want to filter on. So, if the task's
        # Inputs are ModeInputs, dereference the task's Inputs class.
        if issubclass(self.__task.Inputs, vdp.ModeInputs):
            active_mode_task = self.__task.Inputs._modes[self.__inputs.mode]
            task_inputs_cls = active_mode_task.Inputs
        else:
            task_inputs_cls = self.__task.Inputs
        valid_args = validate_args(task_inputs_cls, task_args)

        is_mpi_ready = mpihelpers.is_mpi_ready()
        is_tier0_job = is_mpi_ready

        parallel_wanted = mpihelpers.parse_mpi_input_parameter(self.__inputs.parallel)

        # PIPE-2114: always execute per-EB "SerialTasks" from the MPI client process in a single-EB
        # data processing session.
        if parallel_wanted and len(as_list(self.__inputs.vis)) == 1:
            LOG.debug('Only a single EB is detected in the input vis list; switch to parallel=False '
                      'to execute the task on the MPIclient.')
            parallel_wanted = False

        if is_tier0_job and parallel_wanted:
            executable = mpihelpers.Tier0PipelineTask(self.__task, valid_args, self.__context_path)
            return valid_args, mpihelpers.AsyncTask(executable)
        else:
            inputs = vdp.InputsContainer(self.__task, self.__context, **valid_args)
            task = self.__task(inputs)
            return valid_args, mpihelpers.SyncTask(task, self.__executor)

    def __get_task_args(self, vis):
        inputs = self.__inputs

        original_vis = inputs.vis
        try:
            inputs.vis = vis
            task_args = inputs.as_dict()
            # support for single-dish tasks
            if 'infiles' in task_args:
                task_args['infiles'] = task_args['vis']
        finally:
            inputs.vis = original_vis

        return task_args


def remove_unexpected_args(fn, fn_args):
    # get the argument names for the function
    arg_names = list(signature(fn).parameters)

    # identify arguments that are not expected by the function
    unexpected = [k for k in fn_args if k not in arg_names]

    # return the fn args purged of any unexpected items
    x = {k: v for k, v in fn_args.items() if k not in unexpected}

    # LOG.info('Arg names: {!s}'.format(arg_names))
    # LOG.info('Unexpected: {!s}'.format(unexpected))
    # LOG.info('Valid: {!s}'.format(x))

    return x


def validate_args(inputs_cls, task_args):
    inputs_constructor_fn = getattr(inputs_cls, '__init__')
    valid_args = remove_unexpected_args(inputs_constructor_fn, task_args)
    return valid_args


def get_spwmap(source_ms, target_ms):
    """
    Get a map of spectral windows IDs that map from a source spw ID in
    the source MS to its equivalent spw in the target MS.

    :param source_ms: the MS to map spws from
    :type source_ms: domain.MeasurementSet
    :param target_ms: the MS to map spws to
    :type target_ms: domain.MeasurementSet
    :return: dict of integer spw IDs
    """
    # spw names are not guaranteed to be unique. They seem to be unique
    # across science intents, but they could be repeated for other
    # scans (pointing, sideband, etc.) in non-science spectral windows.
    # if not eliminated, these non-science window names collide with
    # the science windows and you end up having science windows map to
    # non-science windows and vice versa. Not what we want! This set
    # will be used to filter for the spectral windows we want to
    # consider.
    science_intents = {'AMPLITUDE', 'BANDPASS', 'PHASE', 'TARGET', 'CHECK', 'POLARIZATION'}

    # map spw id to spw name for source MS - just for science intents
    id_to_name = {spw.id: spw.name
                  for spw in source_ms.spectral_windows
                  if not science_intents.isdisjoint(spw.intents)}

    # map spw name to spw id for target MS - just for science intents
    name_to_id = {spw.name: spw.id
                  for spw in target_ms.spectral_windows
                  if not science_intents.isdisjoint(spw.intents)}

    # note the get(v, k) here. This says that for non-science windows,
    # which have been filtered from the maps, use the original spw ID -
    # hence non-science spws are not remapped.
    return {k: name_to_id.get(v, k)
            for k, v in id_to_name.items()
            if v in name_to_id}


def remap_spw_int(source_ms, target_ms, spws):
    """
    Map integer spw arguments from one MS to their equivalent spw in
    the target ms.

    :param source_ms: the MS to map spws from
    :type source_ms: domain.MeasurementSet
    :param target_ms: the MS to map spws to
    :type target_ms: domain.MeasurementSet
    :param spws: the spw argument to convert
    :return: a list of remapped integer spw IDs
    :rtype: list
    """
    int_spw_map = get_spwmap(source_ms, target_ms)
    return [int_spw_map[spw_id] for spw_id in spws]


def remap_spw_str(source_ms, target_ms, spws):
    """
    Remap a string spw argument, e.g., '16,18,20,22', from one MS to
    the equivalent map in the target ms.

    :param source_ms: the MS to map spws from
    :type source_ms: domain.MeasurementSet
    :param target_ms: the MS to map spws to
    :type target_ms: domain.MeasurementSet
    :param spws: the spw argument to convert
    :return: a list of remapped integer spw IDs
    :rtype: str
    """
    spw_ints = [int(i) for i in spws.split(',')]
    l = remap_spw_int(source_ms, target_ms, spw_ints)
    return ','.join([str(i) for i in l])


class ParallelTemplate(basetask.StandardTaskTemplate):
    is_multi_vis_task = True

    @abc.abstractproperty
    def Task(self):
        """
        A reference to the :class:`Task` class containing the implementation
        for this pipeline stage.
        """
        raise NotImplementedError

    def __init__(self, inputs):
        super(ParallelTemplate, self).__init__(inputs)

    @basetask.result_finaliser
    def get_result_for_exception(self, vis: str, exception: Exception) -> basetask.FailedTaskResults:
        """Generate FailedTaskResults with exception raised.

        This provides default implementation of exception handling.

        Args:
            vis: List of input visibility data
            exception: Exception occurred

        Return:
            a results object with exception raised
        """
        LOG.error('Error processing {!s}'.format(os.path.basename(vis)))
        LOG.error('{0}({1})'.format(exception.__class__.__name__, str(exception)))
        tb = traceback.format_exc()
        if tb.startswith('None'):
            tb = '{0}({1})'.format(exception.__class__.__name__, str(exception))
        return basetask.FailedTaskResults(self.__class__, exception, tb)

    def prepare(self):
        inputs = self.inputs

        # this will hold the tuples of ms, jobs and results
        assessed = []

        vis_list = as_list(inputs.vis)
        with VDPTaskFactory(inputs, self._executor, self.Task) as factory:
            task_queue = [(vis, factory.get_task(vis)) for vis in vis_list]

            # Jobs must complete within the scope of the VDPTaskFactory as the
            # context copies used by the MPI clients are removed on __exit__.
            for (vis, (task_args, task)) in task_queue:
                try:
                    worker_result = task.get_result()

                    # for importdata/restoredata tasks, input and output vis
                    # can be different. sessionutils seems to require vis to
                    # be output vis
                    if isinstance(worker_result, collections.abc.Iterable):
                        result = worker_result[0]
                    else:
                        result = worker_result
                    if hasattr(result, 'mses'):
                        vis = result.mses[0].name
                except exceptions.PipelineException as e:
                    assessed.append((vis, task_args, e))
                else:
                    assessed.append((vis, task_args, worker_result))

        return assessed

    def analyse(self, assessed):
        # all results will be added to this object
        final_result = basetask.ResultsList()

        context = self.inputs.context

        # if results are generated by importdata task,
        # retrieve measurementset domain objects from the results
        mses = []
        for _, _, vis_result in assessed:
            if isinstance(vis_result, collections.abc.Iterable):
                for r in vis_result:
                    mses.extend(getattr(r, 'mses', []))
            else:
                mses.extend(getattr(vis_result, 'mses', []))

        session_groups = group_into_sessions(context, assessed, measurement_sets=mses)
        for session_id, session_results in session_groups.items():
            for vis, task_args, vis_result in session_results:
                if isinstance(vis_result, Exception):
                    fake_result = self.get_result_for_exception(vis, vis_result)
                    fake_result.inputs = task_args
                    final_result.append(fake_result)

                else:
                    if isinstance(vis_result, collections.abc.Iterable):
                        final_result.extend(vis_result)
                    else:
                        final_result.append(vis_result)

        return final_result
