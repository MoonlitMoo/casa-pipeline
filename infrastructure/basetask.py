import abc
import collections
import copy
import datetime
import functools
import inspect
import matplotlib
import os
import pickle
import re
import textwrap
import traceback
import uuid

from . import api
from . import casa_tools
from . import eventbus
from . import filenamer
from . import jobrequest
from . import logging
from . import pipelineqa
from . import project
from . import task_registry
from . import utils
from . import vdp
from .eventbus import TaskStartedEvent, TaskCompleteEvent, TaskAbnormalExitEvent
from .eventbus import ResultAcceptingEvent, ResultAcceptedEvent, ResultAcceptErrorEvent
from .casa_tasks import CasaTasks

LOG = logging.get_logger(__name__)

# control generation of the weblog
DISABLE_WEBLOG = False
VISLIST_RESET_KEY = '_do_not_reset_vislist'


def timestamp(method):
    def attach_timestamp_to_results(self, *args, **kw):
        start = datetime.datetime.utcnow()
        result = method(self, *args, **kw)
        end = datetime.datetime.utcnow()

        if result is not None:
            result.timestamps = Timestamps(start, end)

        return result

    return attach_timestamp_to_results


def result_finaliser(method):
    """
    Copy some useful properties to the Results object before returning it.
    This is used in conjunction with execute(), where the Result could be
    returned from a number of places but we don't want to set the properties
    in each location.

    TODO: refactor so this is done as part of execute!
    """
    def finalise_pipeline_result(self, *args, **kw):
        result = method(self, *args, **kw)

        if isinstance(result, ResultsList) and len(result) == 0:
            return result

        elif result is not None:
            inputs = self.inputs
            result.inputs = inputs.as_dict()
            result.stage_number = inputs.context.task_counter
            try:
                result.pipeline_casa_task = inputs._pipeline_casa_task
            except AttributeError:
                # sub-tasks may not have pipeline_casa_task, but we only need
                # it for the top-level task
                pass
        return result

    return finalise_pipeline_result


def capture_log(method):
    def capture(self, *args, **kw):
        # get the size of the CASA log before task execution
        logfile = casa_tools.log.logfile()
        size_before = os.path.getsize(logfile)

        # execute the task
        result = method(self, *args, **kw)

        # copy the CASA log entries written since task execution to the result
        with open(logfile, 'r') as casalog:
            casalog.seek(size_before)

            # sometimes we can't write properties, such as for flagdeteralma
            # when the result is a dict
            try:
                result.casalog = casalog.read()
            except:
                LOG.trace('Could not set casalog property on result of type '
                          '%s' % result.__class__)

        # To save space in the pickle, delete any inner CASA logs. The web
        # log will only write the outer CASA log to disk
        if isinstance(result, collections.abc.Iterable):
            for r in result:
                if hasattr(r, 'casalog'):
                    del r.casalog

        return result
    return capture


def matplotlibrc_handler(method):
    @functools.wraps(method)
    def handle_matplotlibrc(self, *args, **kwargs):
        # execute method within dedicated matplotlib context to pipeline
        # currently default rcParams is used
        with matplotlib.rc_context(rc=matplotlib.rcParamsDefault):
            # execute method
            result = method(self, *args, **kwargs)

        return result

    return handle_matplotlibrc


class ModeTask(api.Task):
    # override this if your inputs needs visibility of all measurement sets in
    # scope
    is_multi_vis_task = False

    def __init__(self, inputs):
        super(ModeTask, self).__init__()

        # complain if we were given the wrong type of inputs
        if not isinstance(inputs, self.Inputs):
            msg = '{0} requires inputs of type {1} but got {2}.'.format(
                self.__class__.__name__,
                self.Inputs.__name__,
                inputs.__class__.__name__)
            raise TypeError(msg)

        self.inputs = inputs
        self._delegate = inputs.get_task()

    def execute(self, **parameters):
        self._check_delegate()
        return self._delegate.execute(**parameters)

    def __getattr__(self, name):
        self._check_delegate()
        return getattr(self._delegate, name)

    def _check_delegate(self):
        """
        Update, if necessary, the delegate task so that it matches the
        mode specified in the Inputs.

        This function is necessary as the value of Inputs.mode can change
        after the task has been constructed. Therefore, we cannot rely on any
        delegate set at construction time. Instead, the delegate must be
        updated on every execution.
        """
        # given two modes, A and B, it's possible that A is a subclass of B,
        # eg. PhcorChannelBandpass extends ChannelBandpass, therefore we
        # cannot test whether the current instance is the correct type using
        # isinstance. Instead, we need to compare class names.
        mode = self.inputs.mode
        mode_cls = self.inputs._modes[mode]
        mode_cls_name = mode_cls.__name__

        delegate_cls_name = self._delegate.__class__.__name__

        if mode_cls_name != delegate_cls_name:
            self._delegate = self.inputs.get_task()


# A simple named tuple to hold the start and end timestamps
Timestamps = collections.namedtuple('Timestamps', ['start', 'end'])


class Results(api.Results):
    """
    Results is the base implementation of the Results API.

    In practice, all results objects should subclass this object to take
    advantage of the shared functionality.
    """
    def __init__(self):
        super(Results, self).__init__()

        # set the value used to uniquely identify this object. This value will
        # be used to determine whether this results has already been merged
        # with the context
        self._uuid = uuid.uuid4()

        # property used to hold pipeline QA values
        self.qa = pipelineqa.QAScorePool()

        # property used to hold metadata and presentation-focused values
        # destined for the weblog. Currently a dict, but could change.
        self._metadata = {}

    @property
    def uuid(self):
        """
        The unique identifier for this results object.
        """
        return self._uuid

    @property
    def metadata(self):
        """
        Object holding presentation-related values destined for the web log
        """
        return self._metadata

    def merge_with_context(self, context):
        """
        Merge these results with the given context.

        This method will be called during the execution of accept(). For
        calibration tasks, a typical implementation will register caltables
        with the pipeline callibrary.

        At this point the result is deemed safe to merge, so no further checks
        on the context need be performed.

        :param context: the target
            :class:`~pipeline.infrastructure.launcher.Context`
        :type context: :class:`~pipeline.infrastructure.launcher.Context`
        """
        LOG.debug('Null implementation of merge_with_context used for %s'
                  '' % self.__class__.__name__)

    @matplotlibrc_handler
    def accept(self, context=None):
        """
        Accept these results, registering objects with the context and incrementing
        stage counters as necessary in preparation for the next task.
        """
        event = ResultAcceptingEvent(context_name=context.name, stage_number=self.stage_number)
        eventbus.send_message(event)

        if context is None:
            # context will be none when called from a CASA interactive
            # session. When this happens, we need to locate the global context
            # from the stack
            import pipeline.h.cli.utils
            context = pipeline.h.cli.utils.get_context()

        # find whether this result is being accepted as part of a task
        # execution or whether it's being accepted after task completion
        task_completed = utils.task_depth() == 0

        # Check to ensure this exact result was not already merged into the
        # context.
        self._check_for_remerge(context)

        # If all goes well, this result is the one that will be appended to
        # the results list of the context.
        result_to_append = self

        # PIPE-16: handle exceptions that may occur during the acceptance of
        # the result into the context:
        # Try to execute the task-specific (non-pipeline-framework) parts of
        # results acceptance, which are the merging of the result into the
        # context, and the task QA calculation (for top-level tasks).
        try:
            # execute our template function
            self.merge_with_context(context)

            # perform QA if accepting this result from a top-level task
            if task_completed:
                pipelineqa.qa_registry.do_qa(context, self)

        # If an exception happened during the acceptance of a top-level
        # pipeline task, then create a new FailedTaskResults that contains
        # the exception traceback, and mark this new result as the one
        # to append to the context results list.
        except Exception as ex:
            if task_completed:
                # Log error message.
                tb = traceback.format_exc()
                LOG.error('Error while accepting pipeline task result into context.')
                LOG.error(tb)

                # Create a special result object representing the failed task.
                failedresult = FailedTaskResults(self._get_task(), ex, tb)
                # Copy over necessary properties from real result.
                failedresult.stage_number = self.stage_number
                failedresult.inputs = self.inputs
                failedresult.timestamps = self.timestamps

                # Override the result that is to be appended to the context.
                result_to_append = failedresult

            # Re-raise the exception to allow executioner of pipeline tasks to
            # decide how to proceed.
            raise

        # Whether the result acceptance went ok, or an exception occurred
        # and a new FailedTaskResults was created, always carry out the
        # following steps on the result_to_append, to ensure that e.g.
        # the result is pickled to disk and the weblog is rendered.
        finally:
            if task_completed:
                # If accept() is called at the end of a task, create a proxy for
                # this result and pickle it to the appropriate weblog stage
                # directory. This keeps the context size at a minimum.
                proxy = ResultsProxy(context)
                proxy.write(result_to_append)
                result = proxy
            else:
                result = result_to_append

            # Add the results object to the results list.
            context.results.append(result)

            # having called the old constructor, we know that self.context is set.
            # Use this context to find the report directory and write to the log
            if task_completed:
                # this needs to come before web log generation as the filesizes of
                # various logs and scripts are calculated during web log generation
                write_pipeline_casa_tasks(context)

            # generate weblog if accepting a result from outside a task execution
            if task_completed and not DISABLE_WEBLOG:
                # cannot import at initial import time due to cyclic dependency
                import pipeline.infrastructure.renderer.htmlrenderer as htmlrenderer
                htmlrenderer.WebLogGenerator.render(context)

            # If running at DEBUG loglevel and this is a top-level task result,
            # then store to disk a pickle of the context as it existed at the
            # end of this pipeline stage; this may be useful for debugging.
            if task_completed and LOG.isEnabledFor(logging.DEBUG):
                basename = 'context-stage%s.pickle' % result_to_append.stage_number
                path = os.path.join(context.output_dir,
                                    context.name,
                                    'saved_state',
                                    basename)

                utils.mkdir_p(os.path.dirname(path))
                with open(path, 'wb') as outfile:
                    pickle.dump(context, outfile, -1)

            # Event must be sent from inside finally block to ensure that
            # the event is always sent even if result acceptance is failed.
            if isinstance(result_to_append, FailedTaskResults):
                # result acceptance failed, so we need to send ResultAcceptErrorEvent
                event = ResultAcceptErrorEvent(context_name=context.name, stage_number=self.stage_number)
            else:
                # result acceptance went well, results object must be self
                assert result_to_append is self
                event = ResultAcceptedEvent(context_name=context.name, stage_number=self.stage_number)

            # Sending event should be very last line in this method.
            # Adding any code after sending event is not recommended because
            # the time spent for these lines will not be included in
            # the timing statistics generated by timetracker.
            eventbus.send_message(event)

    def _check_for_remerge(self, context):
        """
        Check whether this result has already been added to the given context.
        """
        # context.results contains the list of results that have been merged
        # with the context. Check whether the UUID of any result or sub-result
        # in that list matches the UUID of this result.
        for result in context.results:
            if self._is_uuid_in_result(result):
                msg = 'This result has already been added to the context'
                LOG.error(msg)
                raise ValueError(msg)

    def _is_uuid_in_result(self, result):
        """
        Return True if the UUID of this result matches the UUID of the given
        result or any sub-result contained within.
        """
        for subtask_result in getattr(result, 'subtask_results', ()):
            if self._is_uuid_in_result(subtask_result):
                return True

        if result.uuid == self.uuid:
            return True

        return False

    def _get_task(self):
        return getattr(self, 'task', None)


class FailedTaskResults(Results):
    """
    FailedTaskResults represents a results object for a task that encountered
    an exception during execution.
    """
    def __init__(self, origtask_cls, exception, tb):
        super(FailedTaskResults, self).__init__()
        self.exception = exception
        self.origtask_cls = origtask_cls
        self.task = FailedTask
        self.tb = tb

    def __repr__(self):
        s = "FailedTaskResults:\n"\
             "\toriginal task: {}\n".format(self.origtask_cls.__name__)
        return s


class ResultsProxy(object):
    def __init__(self, context):
        self._context = context

    def write(self, result):
        """
        Write the pickled result to disk.
        """
        # adopt the result's UUID protecting against repeated addition to the
        # context
        self.uuid = result.uuid

        self._write_stage_logs(result)

        # only store the basename to allow for relocation between save and
        # restore
        self._basename = 'result-stage%s.pickle' % result.stage_number
        path = os.path.join(self._context.output_dir,
                            self._context.name,
                            'saved_state',
                            self._basename)

        utils.mkdir_p(os.path.dirname(path))
        with open(path, 'wb') as outfile:
            pickle.dump(result, outfile, pickle.HIGHEST_PROTOCOL)

    def read(self):
        """
        Read the pickle from disk, returning the unpickled object.
        """
        path = os.path.join(self._context.output_dir,
                            self._context.name,
                            'saved_state',
                            self._basename)
        with open(path, 'rb') as infile:
            return utils.pickle_load(infile)

    def _write_stage_logs(self, result):
        """
        Take the CASA log snippets attached to each result and write them to
        the appropriate weblog directory. The log snippet is deleted from the
        result after a successful write to keep the pickle size down.
        """
        if not hasattr(result, 'casalog'):
            return

        stage_dir = os.path.join(self._context.report_dir,
                                 'stage%s' % result.stage_number)
        if not os.path.exists(stage_dir):
            os.makedirs(stage_dir)

        stagelog_entries = result.casalog
        start = result.timestamps.start
        end = result.timestamps.end

        stagelog_path = os.path.join(stage_dir, 'casapy.log')
        with open(stagelog_path, 'w') as stagelog:
            LOG.debug('Writing CASA log entries for stage %s (%s -> %s)' %
                      (result.stage_number, start, end))
            stagelog.write(stagelog_entries)

        # having written the log entries, the CASA log entries have no
        # further use. Remove them to keep the size of the pickle small
        delattr(result, 'casalog')


class ResultsList(Results):
    def __init__(self, results=None):
        super(ResultsList, self).__init__()
        self.__results = []
        if results:
            self.__results.extend(results)

    def __getitem__(self, item):
        return self.__results[item]

    def __iter__(self):
        return self.__results.__iter__()

    def __len__(self):
        return len(self.__results)

    def __str__(self):
        return 'ResultsList({!s})'.format(str(self.__results))

    def __repr__(self):
        return 'ResultsList({!s})'.format(repr(self.__results))

    def append(self, other):
        self.__results.append(other)

    def accept(self, context=None):
        return super(ResultsList, self).accept(context)

    def extend(self, other):
        for o in other:
            self.append(o)

    def merge_with_context(self, context):
        for result in self.__results:
            result.merge_with_context(context)

    def _get_task(self):
        if len(self) > 0:
            return self[0]._get_task()
        else:
            return None


class StandardTaskTemplate(api.Task, metaclass=abc.ABCMeta):
    """
    StandardTaskTemplate is a template class for pipeline reduction tasks whose
    execution can be described by a common four-step process:

    #. prepare(): examine the measurement set and prepare a list of\
        intermediate job requests to be executed.
    #. execute the jobs
    #. analyze(): analyze the output of the intermediate job requests,\
        deciding if necessary which parameters provide the best results, and\
        return these results.
    #. return a final list of jobs to be executed using these best-fit\
        parameters.

    Simpletask implements the :class:`Task` interface and steps 2 and 4 in the
    above process, leaving subclasses to implement
    :func:`~SimpleTask.prepare` and :func:`~SimpleTask.analyse`.


    A Task and its :class:`Inputs` are closely aligned. It is anticipated that
    the Inputs for a Task will be created using the :attr:`Task.Inputs`
    reference rather than locating and instantiating the partner class
    directly, eg.::

        i = ImplementingTask.Inputs.create_from_context(context)

    """

    # HeadTail is an internal class used to associate properties with their
    # associated measurement sets
    HeadTail = collections.namedtuple('HeadTail', ('head', 'tail'))

    def __init__(self, inputs):
        """
        Create a new Task with an initial state based on the given inputs.

        :param Inputs inputs: inputs required for this Task.
        """
        super(StandardTaskTemplate, self).__init__()

        # complain if we were given the wrong type of inputs
        if isinstance(inputs, vdp.InputsContainer):
            error = (inputs._task_cls.Inputs != self.Inputs)
        else:
            error = not isinstance(inputs, self.Inputs)

        if error:
            msg = '{0} requires inputs of type {1} but got {2}.'.format(
                self.__class__.__name__,
                self.Inputs.__name__,
                inputs.__class__.__name__)
            raise TypeError(msg)

        self.inputs = inputs

    is_multi_vis_task = False

    @abc.abstractmethod
    def prepare(self, **parameters):
        """
        Prepare job requests for execution.

        :param parameters: the parameters to pass through to the subclass.
            Refer to the implementing subclass for specific information on
            what these parameters are.
        :rtype: a class implementing :class:`~pipeline.api.Result`
        """
        raise NotImplementedError

    @abc.abstractmethod
    def analyse(self, result):
        """Analyze the result from the task preparing operation.

        This method analyzes the result object returned by the Pipeline task.prepare method
        and returns it with optional modifications.

        :param result: the task result object generated by :func:`~SimpleTask.prepare`
        :rtype: :class:`~pipeline.api.Result`
        """
        raise NotImplementedError

    @timestamp
    @matplotlibrc_handler
    @capture_log
    @result_finaliser
    def execute(self, **parameters):
        if utils.is_top_level_task():
            # Set the task name, but only if this is a top-level task. This
            # name will be prepended to every data product name as a sign of
            # their origin
            try:
                name = task_registry.get_casa_task(self.__class__)
            except KeyError:
                name = self.__class__.__name__
            filenamer.NamingTemplate.task = name

            # initialise the subtask counter, which will be subsequently
            # incremented for every execute within this top-level task
            self.inputs.context.task_counter += 1
            LOG.info('Starting execution for stage %s',
                     self.inputs.context.task_counter)
            self.inputs.context.subtask_counter = 0

            event = TaskStartedEvent(context_name=self.inputs.context.name,
                                     stage_number=self.inputs.context.task_counter)
            eventbus.send_message(event)

            # log the invoked pipeline task and its comment to
            # casa_commands.log
            _log_task(self)

        else:
            self.inputs.context.subtask_counter += 1

        # Create a copy of the inputs - including the context - and attach
        # this copy to the Inputs. Tasks can then merge results with this
        # duplicate context at will, as we'll later restore the originals.
        original_inputs = self.inputs
        self.inputs = utils.pickle_copy(original_inputs)

        # create a job executor that tasks can use to execute subtasks
        self._executor = Executor(self.inputs.context)
        self._executable = CasaTasks(executor=self._executor)

        # create a new log handler that will capture all messages above
        # ATTENTION level.
        handler = logging.CapturingHandler(logging.ATTENTION)

        try:
            # if this task does not handle multiple input mses but was
            # invoked with multiple mses in its inputs, call our utility
            # function to invoke the task once per ms.
            if not self.is_multi_vis_task:
                if isinstance(self.inputs, vdp.InputsContainer) or isinstance(self.inputs.vis, list):
                    return self._handle_multiple_vis(**parameters)

            if isinstance(self.inputs, vdp.InputsContainer) and self.inputs._pipeline_casa_task is not None:
                LOG.info('Equivalent Pipeline CLI call: %s', self.inputs._pipeline_casa_task)

            # We should not pass unused parameters to prepare(), so first
            # inspect the signature to find the names the arguments and then
            # create a dictionary containing only those parameters
            prepare_args = set(inspect.getfullargspec(self.prepare).args)
            prepare_parameters = dict(parameters)
            for arg in parameters:
                if arg not in prepare_args:
                    del prepare_parameters[arg]

            # register the capturing log handler, buffering all messages so that
            # we can add them to the result - and subsequently, the weblog
            logging.add_handler(handler)

            # get our result
            result = self.prepare(**prepare_parameters)

            # analyse them..
            result = self.analyse(result)

            # tag the result with the class of the originating task
            result.task = self.__class__

            # add the log records to the result
            if not hasattr(result, 'logrecords'):
                result.logrecords = handler.buffer
            else:
                result.logrecords.extend(handler.buffer)

            event = TaskCompleteEvent(context_name=self.inputs.context.name,
                                      stage_number=self.inputs.context.task_counter)
            eventbus.send_message(event)

            return result

        except Exception as ex:
            # Created a special result object for the failed task, but only if
            # this is a top-level task; otherwise, raise the exception higher
            # up.
            if utils.is_top_level_task():
                # Get the task name from the task registry, otherwise use the
                # task class name.
                try:
                    name = task_registry.get_casa_task(self.__class__)
                except KeyError:
                    name = self.__class__.__name__

                # Log error message.
                tb = traceback.format_exc()
                LOG.error('Error executing pipeline task %s.' % name)
                LOG.error(tb)

                # Create a special result object representing the failed task.
                result = FailedTaskResults(self.__class__, ex, tb)

                # add the log records to the result
                if not hasattr(result, 'logrecords'):
                    result.logrecords = handler.buffer
                else:
                    result.logrecords.extend(handler.buffer)

                event = TaskAbnormalExitEvent(context_name=self.inputs.context.name,
                                              stage_number=self.inputs.context.task_counter)
                eventbus.send_message(event)

                return result
            else:
                raise
        finally:
            # restore the context to the original context
            self.inputs = original_inputs

            # delete the task name once the top-level task is complete
            if utils.is_top_level_task():
                filenamer.NamingTemplate.task = None

            # now that the WARNING and above messages have been attached to the
            # result, remove the capturing logging handler from all loggers
            if handler:
                logging.remove_handler(handler)

            # delete the executor so that the pickled context can be released
            self._executor = None
            self._executable = None

    def _handle_multiple_vis(self, **parameters):
        """
        Handle a single task invoked for multiple measurement sets.

        This function handles the case when the vis parameter on the Inputs
        specifies multiple measurement sets. In this situation, we want to
        invoke the task for each individual MS. We could do this by having
        each task iterate over the measurement sets involved, but in order to
        keep the task implementations as simple as possible, that complexity
        (unless overridden) is handled by the template task instead.

        If the task wants to handle the multiple measurement sets
        itself it should override is_multi_vis_task.
        """
        # The following code loops through the MSes specified in vis,
        # executing the task for the first value (head) and then appending the
        # results of executing the remainder of the MS list (tail).
        if len(self.inputs.vis) == 0:
            # we don't return an empty list as the timestamp decorator wants
            # to set attributes on this value, which it can't on a built-in
            # list
            return ResultsList()

        container = self.inputs
        if container._pipeline_casa_task is not None:
            LOG.info('Equivalent Pipeline CLI call: %s', container._pipeline_casa_task)

        results = ResultsList()
        try:
            for inputs in container:
                self.inputs = inputs
                single_result = self.execute(**parameters)

                if isinstance(single_result, ResultsList):
                    results.extend(single_result)
                else:
                    results.append(single_result)
            return results
        finally:
            self.inputs = container


    def _get_handled_headtails(self, names=None):
        handled = collections.OrderedDict()

        if names is None:
            # no names to get so return empty dict
            return handled

        for name in names:
            if hasattr(self.inputs, name):
                property_value = getattr(self.inputs, name)

                head = property_value[0]
                tail = property_value[1:]
                ht = StandardTaskTemplate.HeadTail(head=head, tail=tail)

                handled[name] = ht

        return handled


class FailedTask(StandardTaskTemplate):
    def __init__(self, context):
        inputs = vdp.InputsContainer(self, context)
        super(FailedTask, self).__init__(inputs)


class Executor(object):
    def __init__(self, context):
        self._context = context
        self._output_dir = self._context.output_dir
        self.cmdfile = os.path.join(context.report_dir,
                                    context.logs['casa_commands'])

    @property
    def cmdfile(self):
        return self._cmdfile

    @cmdfile.setter
    def cmdfile(self, value):
        if isinstance(value, str):
            if hasattr(self, '_cmdfile'):
                LOG.debug(f'Switch the CASA commands log file from {self._cmdfile} to {value}.')
            self._cmdfile = os.path.abspath(value)

    @capture_log
    def execute(self, job, merge=False, **kwargs):
        """
        Execute the given job or subtask, returning its output.

        :param job: a job or subtask
        :type job: an object conforming to the :class:`~pipeline.api.Task`\
            interface

        :rtype: :class:`~pipeline.api.Result`
        """
        # execute the job, capturing its results object
        result = job.execute(**kwargs)

        # if the job was a JobRequest, log it to our command log
        if isinstance(job, jobrequest.JobRequest):
            self._log_jobrequest(job)

        # if requested, merge the result with the context.
        if merge:
            if self._context is None:
                # PIPE-1522: A "context-free" copy of the Executor instance can be created from the 'copy'
                # method (exclude_context=True), and sent to MPI servers for Tier0JobRequest executions.
                # This reduces the MPI message size.
                LOG.error('The context object is detached from the Executor instance and \
                            the job/subtask result merge is not allowed.')
            else:
                result.accept(self._context)

        return result

    def _log_jobrequest(self, job):
        # CAS-5262: casa_commands.log written by the pipeline should
        # be formatted to be more easily readable.

        # If the output directory is set to a valid string, then replace any
        # occurrence of this output path in arguments with an empty string, to
        # ensure the casa commands log does not contain hardcoded paths
        # specific to where the pipeline ran.
        if os.path.isdir(self._output_dir):
            job_str = re.sub(r'%s/' % self._output_dir, '', str(job))
        else:
            job_str = str(job)

        # wrap the text at the first open bracket
        if '(' in job_str:
            indent = (1+job_str.index('(')) * ' '
        else:
            indent = 10 * ' '

        wrapped = textwrap.wrap(job_str,
                                subsequent_indent=indent,
                                width=80,
                                break_long_words=False)

        with open(self._cmdfile, 'a') as cmdfile:
            cmdfile.write('%s\n' % '\n'.join(wrapped))

    def copy(self, exclude_context=False):
        """Return a shallow copy of the Executor instance.

        Optionally, we can exclude the "context" object to reduce the return object
        size (e.g. when pushing it from the MPI client to the server). However, in that
        case, merge=True will not be allowed in the "execute" method.
        """

        executor_copy = copy.copy(self)
        if exclude_context:
            executor_copy._context = None
        # executor_copy = utils.pickle_copy(executor_copy)
        return executor_copy


def _log_task(task):
    context = task.inputs.context
    filename = os.path.join(context.report_dir,
                            context.logs['casa_commands'])
    comment = ''

    if not os.path.exists(filename):
        wrapped = textwrap.wrap('# ' + _CASA_COMMANDS_PROLOGUE,
                                subsequent_indent='# ',
                                width=78,
                                break_long_words=False)
        comment = ('raise Error(\'The casa commands log is not executable!\')\n'
                   '\n%s\n' % '\n'.join(wrapped))

    comment += '\n# %s\n#\n' % getattr(task.inputs, '_pipeline_casa_task', 'unknown pipeline task')

    # get the description of how this task functions and add it to the comment
    comment += task_registry.get_comment(task.__class__)

    with open(filename, 'a') as cmdfile:
        cmdfile.write(comment)


def property_with_default(name, default, doc=None):
    """
    Return a property whose value is reset to a default value when setting the
    property value to None.
    """
    # our standard name for the private property backing the public property
    # is a prefix of one underscore
    varname = '_' + name

    def getx(self):
        return object.__getattribute__(self, varname)

    def setx(self, value):
        if value is None:
            value = default
        object.__setattr__(self, varname, value)
#    def delx(self):
#        object.__delattr__(self, varname)
    return property(getx, setx, None, doc)


def write_pipeline_casa_tasks(context):
    """
    Write the equivalent pipeline CASA tasks for results in the context to a
    file
    """
    pipeline_tasks = []
    for proxy in context.results:
        result = proxy.read()
        try:
            pipeline_tasks.append(result.pipeline_casa_task)
        except AttributeError:
            pipeline_tasks.append('# stage %s: unknown task generated %s '
                                  'result' % (result.stage_number,
                                              result.__class__.__name__))

    task_string = '\n'.join(['    %s' % t for t in pipeline_tasks])
    # replace the working directory with ''
    if os.path.isdir(context.output_dir):
        task_string = re.sub(r'%s/' % context.output_dir, '', task_string)

    state_commands = []
    for o in (context.project_summary, context.project_structure, context.project_performance_parameters):
        state_commands += ['context.set_state({!r}, {!r}, {!r})'.format(cls, name, value)
                           for cls, name, value in project.get_state(o)]

    template = '''context = h_init()
%s
try:
%s
finally:
    h_save()
''' % ('\n'.join(state_commands), task_string)

    f = os.path.join(context.report_dir, context.logs['pipeline_script'])
    with open(f, 'w') as casatask_file:
        casatask_file.write(template)


_CASA_COMMANDS_PROLOGUE = (
    'This file contains CASA commands run by the pipeline. Although all commands required to calibrate the data are '
    'included here, this file cannot be executed, nor does it contain heuristic and flagging calculations performed by '
    'pipeline code. This file is useful to understand which CASA commands are being run by each pipeline task. If one '
    'wishes to re-run the pipeline, one should use the pipeline script linked on the front page or By Task page of the '
    'weblog. Some stages may not have any commands listed here, e.g. hifa_importdata if conversion from ASDM to MS is '
    'not required.'
)
