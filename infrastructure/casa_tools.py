import contextlib
import copyreg
import datetime
import inspect
import operator
import os
import platform
import sys
from inspect import signature

import casatools
from casatasks import casalog

from . import logging

# standard logger that emits messages to stdout and CASA logger
LOG = logging.get_logger(__name__)

# logger for keeping a trace of CASA task and CASA tool calls.
# The filename incorporates the hostname to keep MPI client files distinct
CASACALLS_LOG = logging.get_logger('CASACALLS', stream=None, format='%(message)s', addToCasaLog=False,
                                   filename='casacalls-{!s}.txt'.format(platform.node().split('.')[0]))


def log_call(fn, level):
    """
    Decorate a function or method so that all invocations of that function or
    method are logged.

    :param fn: function to decorate
    :param level: log level (e.g., logging.INFO, logging.WARNING, etc.)
    :return: decorated function
    """
    def f(*args, **kwargs):
        # remove any keyword arguments that have a value of None or an empty
        # string, letting CASA use the default value for that argument
        kwargs = {k: v for k, v in kwargs.items()
                  if v is not None or (isinstance(v, str) and not v)}

        # get the argument names and default argument values for the given
        # function
        argnames = list(signature(fn).parameters)
        argcount = len(argnames)

        positional = {k: v for k, v in zip(argnames, args)}

        def format_arg_value(arg_val):
            arg, val = arg_val
            return '%s=%r' % (arg, val)

        nameless = list(map(repr, args[argcount:]))
        positional = list(map(format_arg_value, iter(positional.items())))
        keyword = list(map(format_arg_value, iter(kwargs.items())))

        # don't want self in message as it is an object memory reference
        msg_args = [v for v in positional + nameless + keyword if not v.startswith('self=')]

        tool_call = '{!s}.{!s}({!s})'.format(fn.__module__, fn.__name__, ', '.join(msg_args))
        CASACALLS_LOG.log(level, tool_call)

        start_time = datetime.datetime.utcnow()
        try:
            return fn(*args, **kwargs)
        finally:
            end_time = datetime.datetime.utcnow()
            elapsed = end_time - start_time
            LOG.log(level, '{} CASA tool call took {}s'.format(tool_call, elapsed.total_seconds()))

    return f


def create_logging_class(cls, level=logging.TRACE, to_log=None):
    """
    Return a class with all methods decorated to log method calls.

    :param cls: class to wrap
    :param level: log level for emitted messages
    :param to_log: methods to log calls for, or None to log all methods
    :return: the decorated class
    """
    bound_methods = {name: method
                     for (name, method) in inspect.getmembers(cls, inspect.isfunction)}

    if to_log:
        bound_methods = {name: method for name, method in bound_methods.items() if name in to_log}

    logging_override_methods = {name: log_call(method, level)
                                for name, method in bound_methods.items()}

    cls_name = 'Logging{!s}'.format(cls.__name__.capitalize())
    new_cls = type(cls_name, (cls,), logging_override_methods)

    return new_cls


# wrappers around CASA tool classes that create CASA tools where method calls
# are logged with log level Y.
# The assigned log level for tools should be DEBUG or lower, otherwise the log
# file is created and written to even on non-debug pipeline runs, where
# loglevel=INFO. The default log level is TRACE.
#
# Example:
_logging_atmosphere_cls = create_logging_class(casatools.atmosphere)
_logging_calanalysis_cls = create_logging_class(casatools.calanalysis)
_logging_calibrater_cls = create_logging_class(casatools.calibrater)
_logging_image_cls = create_logging_class(casatools.image)
_logging_imagepol_cls = create_logging_class(casatools.imagepol)
_logging_imager_cls = create_logging_class(casatools.imager,
                                           level=logging.INFO, to_log=('selectvis', 'apparentsens', 'advise'))
_logging_logsink_cls = create_logging_class(casatools.logsink)
_logging_measures_cls = create_logging_class(casatools.measures)
_logging_ms_cls = create_logging_class(casatools.ms)
_logging_msmd_cls = create_logging_class(casatools.msmetadata)
_logging_quanta_cls = create_logging_class(casatools.quanta)
_logging_regionmanager_cls = create_logging_class(casatools.regionmanager)
_logging_synthesisutils_cls = create_logging_class(casatools.synthesisutils)
_logging_table_cls = create_logging_class(casatools.table)
_logging_utils_cls = create_logging_class(casatools.utils.utils)

atmosphere = _logging_atmosphere_cls()
calanalysis = _logging_calanalysis_cls()
calibrater = _logging_calibrater_cls()
image = _logging_image_cls()
imagepol = _logging_imagepol_cls()
imager = _logging_imager_cls()
logsink = _logging_logsink_cls()
measures = _logging_measures_cls()
ms = _logging_ms_cls()
msmd = _logging_msmd_cls()
quanta = _logging_quanta_cls()
regionmanager = _logging_regionmanager_cls()
synthesisutils = _logging_synthesisutils_cls()
table = _logging_table_cls()
utils = _logging_utils_cls()

log = casalog


def post_to_log(comment='', echo_to_screen=True):
    log.post(comment)
    if echo_to_screen:
        sys.stdout.write('{0}\n'.format(comment))


def set_log_origin(fromwhere=''):
    log.origin(fromwhere)


def context_manager_factory(tool_cls, finalisers=None):
    """
    Create a context manager function that wraps the given CASA tool.

    The returned context manager function takes one argument: a filename. The
    function opens the file using the CASA tool, returning the tool so that it
    may be used for queries or other operations pertaining to the tool. The
    tool is closed once it falls out of scope or an exception is raised.
    """
    tool_name = tool_cls.__name__

    if finalisers is None:
        finalisers = ('close', 'done')

    @contextlib.contextmanager
    def f(filename, **kwargs):
        if not os.path.exists(filename):
            raise IOError('No such file or directory: {!r}'.format(filename))
        LOG.trace('%s tool: opening %r', tool_name, filename)
        tool_instance = tool_cls()
        tool_instance.open(filename, **kwargs)
        try:
            yield tool_instance
        finally:
            LOG.trace('{!s} tool: closing {!r}'.format(tool_name, filename))
            for method_name in finalisers:
                if hasattr(tool_instance, method_name):
                    m = operator.methodcaller(method_name)
                    m(tool_instance)
    return f


def selectvis_context_manager(tool_cls):
    """
    Create an imager tool context manager function that opens the MS using
    im.selectvis in read-only mode.

    The returned context manager function takes one argument: a filename. The
    function opens the file using the CASA imager tool, returning the tool so that it
    may be used for queries or other operations pertaining to the tool. The
    tool is closed once it falls out of scope or an exception is raised.
    """
    tool_name = tool_cls.__name__

    @contextlib.contextmanager
    def f(filename, **kwargs):
        if not os.path.exists(filename):
            raise IOError('No such file or directory: {!r}'.format(filename))
        LOG.trace('{!s} tool: opening {!r} using .selectvis(writeaccess=False)'.format(tool_name, filename))
        tool_instance = tool_cls()
        rtn = tool_instance.selectvis(filename, writeaccess=False, **kwargs)
        if rtn is False:
            raise Exception('selectvis did not return any data')
        try:
            yield tool_instance
        finally:
            LOG.trace('{!s} tool: closing {!r}'.format(tool_name, filename))
            if hasattr(tool_instance, 'close'):
                tool_instance.close()
            if hasattr(tool_instance, 'done'):
                tool_instance.done()

    return f


# context managers for frequently used CASA tools
CalAnalysis = context_manager_factory(_logging_calanalysis_cls)
ImageReader = context_manager_factory(_logging_image_cls)
MSReader = context_manager_factory(_logging_ms_cls, finalisers=['done'])
TableReader = context_manager_factory(_logging_table_cls, finalisers=['done'])
MSMDReader = context_manager_factory(_logging_msmd_cls)
SelectvisReader = selectvis_context_manager(_logging_imager_cls)
ImagepolReader = context_manager_factory(_logging_imagepol_cls)

# C extensions cannot be pickled, so ignore the CASA logger on pickle and
# replace with it with the current CASA logger on unpickle
__tools = [
    'atmosphere',
    'calibrater',
    'image',
    'imagepol',
    'imager',
    'log',
    'measures',
    'ms',
    'quanta',
    'regionmanager',
    'table',
    'utils',
]

for tool in __tools:
    tool_type = type(globals()[tool])
    unpickler = lambda data: globals()[tool]
    pickler = lambda _: (unpickler, (tool, ))
    copyreg.pickle(tool_type, pickler, unpickler)
