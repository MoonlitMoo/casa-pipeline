"""
recipereducer is a utility to reduce data using a standard pipeline procedure.
It parses a XML reduction recipe, converts it to pipeline tasks, and executes
the tasks for the given data. It was written to give pipeline developers
without access to PPRs and/or a PPR generator a way to reduce data using the
latest standard recipe.

Note: multiple input datasets can be specified. Doing so will reduce the data
      as part of the same session.

Example #1: process uid123.tar.gz using the standard recipe.

    import pipeline.recipereducer
    pipeline.recipereducer.reduce(vis=['uid123.tar.gz'])

Example #2: process uid123.tar.gz using a named recipe.

    import pipeline.recipereducer
    pipeline.recipereducer.reduce(vis=['uid123.tar.gz'],
                                  procedure='procedure_hif.xml')

Example #3: process uid123.tar.gz and uid124.tar.gz using the standard recipe.

    import pipeline.recipereducer
    pipeline.recipereducer.reduce(vis=['uid123.tar.gz', 'uid124.tar.gz'])

Example #4: process uid123.tar.gz, naming the context 'testrun', thus
            directing all weblog output to a directory called 'testrun'.

    import pipeline.recipereducer
    pipeline.recipereducer.reduce(vis=['uid123.tar.gz'], name='testrun')

Example #5: process uid123.tar.gz with a log level of TRACE

    import pipeline.recipereducer
    pipeline.recipereducer.reduce(vis=['uid123.tar.gz'], loglevel='trace')

"""
import collections
import os
import tempfile
import traceback
from typing import Any, Callable, List, Optional, Tuple
import xml.etree.ElementTree as ElementTree

import pkg_resources

import pipeline.cli as cli
import pipeline.h.cli.cli as h_cli
import pipeline.infrastructure.launcher as launcher
import pipeline.infrastructure.logging as logging
from pipeline.infrastructure import exceptions, utils

LOG = logging.get_logger(__name__)

RECIPES_DIR = pkg_resources.resource_filename(__name__, 'recipes')

TaskArgs = collections.namedtuple('TaskArgs', 'vis infiles session')


def _create_context(loglevel: str, plotlevel: str, name: str) -> launcher.Context:
    """Create Pipeline context.

    Args:
        loglevel: Logging level
        plotlevel: Plot level
        name: Name of the context

    Returns:
        Pipeline context object
    """
    pipeline = launcher.Pipeline(loglevel=loglevel, plotlevel=plotlevel,
                                 name=name)
    return pipeline.context


def _register_context(loglevel: str, plotlevel: str, context: launcher.Context) -> None:
    """Register given context to global scope.

    If pipeline context already exists in global scope, it is saved on
    disk to avoid being overwritten.

    Args:
        loglevel: Logging level
        plotlevel: Plot level
        context: Pipeline context object
    """
    # check if global context exists
    pipeline_instance = h_cli.stack.get(h_cli.PIPELINE_NAME, None)
    if pipeline_instance and isinstance(pipeline_instance, launcher.Pipeline):
        # if global context exists, check identity with given context
        global_context = pipeline_instance.context
        if global_context == context:
            # context is already registered
            return
        elif global_context.name != context.name:
            # global context is different from given context
            # save global context with intrinsic name
            global_context.save()
            context_file = f'{global_context.name}.context'
            LOG.info(f'Global context exists. Saved it {context_file} to disk.')
        else:
            # global context is different from given context but
            # they accidentally have the same name
            # save global context with different name to avoid
            # name conflict with new one
            for i in range(10):
                context_file = f'{global_context.name}-{i}.context'
                if not os.path.exists(context_file):
                    global_context.save(context_file)
                    LOG.info(f'Global context exists. Saved it {context_file} to disk.')
                    break
            else:
                # failed attempt to find appropriate context name
                # it should rarely happen, but overwrite existing context
                # if it happened
                LOG.warning('Existing Pipeline context will be overridden by the current pipeline processing.')

    # register given context to global scope
    with tempfile.TemporaryDirectory(dir='.') as temp_dir:
        context_name = os.path.join(temp_dir, context.name)
        try:
            # to disable some log messages during registration
            temp_loglevel = 'error'
            logging.set_logging_level(level=temp_loglevel)
            context.save(context_name)
            # create pipeline instance using temporary context
            pipeline_instance = launcher.Pipeline(
                loglevel=temp_loglevel, plotlevel=plotlevel,
                context=context_name
            )
            # then, replace context
            pipeline_instance.context = context
            h_cli.stack[h_cli.PIPELINE_NAME] = pipeline_instance
        finally:
            # set user-specified loglevel
            logging.set_logging_level(level=loglevel)


def _get_context_name(procedure: str) -> str:
    root, _ = os.path.splitext(os.path.basename(procedure))
    return 'pipeline-%s' % root


def _get_processing_procedure(procedure: str) -> ElementTree:
    # find the procedure file on disk, then fall back to the standard recipes
    if os.path.exists(procedure):
        procedure_file = os.path.abspath(procedure)
    else:
        procedure_file = os.path.join(RECIPES_DIR, procedure)
    if os.path.exists(procedure_file):
        LOG.info('Using procedure file: %s' % procedure_file)
    else:
        msg = f'Procedure not found:: {procedure}'
        LOG.error(msg)
        raise IOError(msg)

    procedure_xml = ElementTree.parse(procedure_file)
    if not procedure_xml:
        msg = f'Could not parse procedure file at {procedure_file}'
        LOG.error(msg)
        raise IOError(msg)

    return procedure_xml


def _get_procedure_title(procedure: str) -> str:
    procedure_xml = _get_processing_procedure(procedure)
    procedure_title = procedure_xml.findtext('ProcedureTitle', default='Undefined')
    return procedure_title


def _get_tasks(context: launcher.Context, args: TaskArgs, procedure: str):
    procedure_xml = _get_processing_procedure(procedure)

    commands_seen = []
    for processingcommand in procedure_xml.findall('ProcessingCommand'):
        cli_command = processingcommand.findtext('Command')
        commands_seen.append(cli_command)

        # ignore breakpoints
        if cli_command == 'breakpoint':
            continue

        # skip exportdata when preceded by a breakpoint
        if len(commands_seen) > 1 and commands_seen[-2] == ['breakpoint'] \
                and cli_command == 'hif_exportdata':
            continue

        task_args = {}

        if cli_command in ['h_importdata',
                           'hif_importdata',
                           'hifa_importdata',
                           'hifv_importdata',
                           'hsd_importdata',
                           'hsdn_importdata',
                           'h_restoredata',
                           'hif_restoredata',
                           'hifa_restoredata',
                           'hifv_restoredata',
                           'hsd_restoredata']:
            task_args['vis'] = args.vis
            # we might override this later with the procedure definition
            task_args['session'] = args.session

        elif cli_command in [ 'hsdn_restoredata' ]:
            task_args['vis'] = args.vis

        for parameterset in processingcommand.findall('ParameterSet'):
            for parameter in parameterset.findall('Parameter'):
                argname = parameter.findtext('Keyword')
                argval = parameter.findtext('Value')
                task_args[argname] = utils.string_to_val(argval)

        # we yield rather than return so that the context can be updated
        # between task executions
        task = cli.get_pipeline_task_with_name(cli_command)
        yield task, task_args


def _format_arg_value(arg_val: Tuple[Any, Any]) -> str:
    arg, val = arg_val
    return '%s=%r' % (arg, val)


def _as_task_call(task_func: Callable, task_args: dict) -> str:
    kw_args = list(map(_format_arg_value, task_args.items()))
    return '%s(%s)' % (task_func.__name__, ', '.join(kw_args))


def reduce(vis: Optional[List[str]] = None, infiles: Optional[List[str]] = None,
           procedure: str = 'procedure_hifa_calimage.xml',
           context: Optional[launcher.Context] = None, name: Optional[str] = None,
           loglevel: str = 'info', plotlevel: str = 'default',
           session: Optional[List[str]] = None, exitstage: Optional[int] = None,
           startstage: Optional[int] = None) -> launcher.Context:
    if vis is None:
        vis = []

    if infiles is None:
        infiles = []

    if context is None:
        name = name if name else _get_context_name(procedure)
        context = _create_context(loglevel, plotlevel, name)
        procedure_title = _get_procedure_title(procedure)
        context.set_state('ProjectStructure', 'recipe_name', procedure_title)

    _register_context(loglevel, plotlevel, context)

    if session is None:
        session = ['default'] * len(vis)

    if startstage is None:
        startstage = 0

    task_args = TaskArgs(vis, infiles, session)
    task_generator = _get_tasks(context, task_args, procedure)
    try:
        procedure_stage_nr = 0
        while True:
            task, task_args = next(task_generator)
            procedure_stage_nr += 1
            if procedure_stage_nr < startstage:
                continue
            LOG.info('Executing pipeline task %s' % _as_task_call(task, task_args))

            try:
                result = task(**task_args)
            except Exception as ex:
                # Log message if an exception occurred that was not handled by
                # standardtask template (not turned into failed task result).
                _hif_call = _as_task_call(task, task_args)
                LOG.error('Unhandled error in recipereducer while running pipeline task %s.' % _hif_call)
                traceback.print_exc()
                return context

            tracebacks = utils.get_tracebacks(result)
            if len(tracebacks) > 0:
                previous_tracebacks_as_string = "{}".format("\n".join([tb for tb in tracebacks]))
                raise exceptions.PipelineException(previous_tracebacks_as_string)
            elif result.stage_number is exitstage:
                break

    except StopIteration:
        pass
    finally:
        LOG.info('Saving context...')
        cli.h_save()

    return context
