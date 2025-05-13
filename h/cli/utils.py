from functools import wraps
from typing import Callable

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
import pipeline.infrastructure.argmapper as argmapper
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import exceptions, task_registry, utils
from . import cli
from .. import heuristics

LOG = infrastructure.get_logger(__name__)


def cli_wrapper(func: Callable):
    """Wrap pipeline task CLI functions to handle the extra 'pipelinemode' argument.

    PIPE-1884: this decorator function removes the "pipelinemode" argument from pipeline CLI task
    calls, which commonly exists in archival casa_pipescript.py/casa_pipestorescript.py scripts generated
    by old pipeline versions before PIPE-1686.
    """
    @wraps(func)
    def wrapped_func(*args, **kwargs):
        if 'pipelinemode' in kwargs:
            LOG.attention('The pipeline task argument "pipelinemode" does not affect results and will be removed in the future.')
            kwargs.pop('pipelinemode')
        return func(*args, **kwargs)
    return wrapped_func


def get_context():
    return cli.stack[cli.PIPELINE_NAME].context


def get_output_dir():
    context = get_context()
    return context.output_dir


def get_ms(vis):
    context = get_context()
    return context.observing_run.get_ms(name=vis)


def get_heuristic(arg):
    if issubclass(arg, api.Heuristic):
        return arg()

    if callable(arg):
        return arg

    # TODO LOOK IN HEURISTICS MODULE

    # If the argument is a non-empty string, try to get the class with the
    # given name, or if that class doesn't exist, wrap the input in an
    # EchoHeuristic
    if isinstance(arg, str) and arg:
        packages = arg.split('.')
        module = '.'.join(packages[:-1])
        # if arg was a raw string with no dots, module is empty
        if not module:
            return heuristics.EchoHeuristic(arg)

        try:
            m = __import__(module)
        except ImportError:
            return heuristics.EchoHeuristic(arg)
        for package in packages[1:]:
            m = getattr(m, package, heuristics.EchoHeuristic(arg))
        return m()

    return heuristics.EchoHeuristic(arg)


def execute_task(context, casa_task, casa_args):
    # get the pipeline task inputs
    task_inputs = _get_task_inputs(casa_task, context, casa_args)

    # Execute the class, collecting the results
    results = _execute_task(casa_task, task_inputs)

    # write the command invoked (eg. hif_setjy) to the result so that the
    # weblog can print help from the XML task definition rather than the
    # python class
    results.taskname = casa_task

    # accept the results if desired
    _merge_results(context, results)

    tracebacks = utils.get_tracebacks(results)
    if len(tracebacks) > 0:
        previous_tracebacks_as_string = "{}".format("\n".join([tb for tb in tracebacks]))
        raise exceptions.PipelineException(previous_tracebacks_as_string)

    return results


def _get_task_inputs(casa_task, context, casa_args):
    # convert the CASA arguments to pipeline arguments, renaming and
    # converting as necessary.
    pipeline_task_class = task_registry.get_pipeline_class_for_task(casa_task)
    task_args = argmapper.convert_args(pipeline_task_class, casa_args)
    inputs = vdp.InputsContainer(pipeline_task_class, context, **task_args)

    return inputs


def _execute_task(casa_task, task_inputs):
    # Given the class and CASA name of the stage and the list
    # of stage arguments, compute and return the results.

    # Find the task and run it
    pipeline_task_cls = task_registry.get_pipeline_class_for_task(casa_task)
    task = pipeline_task_cls(task_inputs)

    # Reporting stuff goes here

    # Error checking ?
    return task.execute()


def _merge_results(context, results):
    try:
        results.accept(context)
    except Exception:
        LOG.critical('Warning: Check merge to context for {}'.format(results.__class__.__name__))
        raise
