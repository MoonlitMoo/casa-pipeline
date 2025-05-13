import copy
import functools
import itertools
import operator
import os
import platform
import re
import sys
import types
from inspect import signature

import casatasks
import casaplotms

# PIPE-2099: add the compatibility with the 'wvrgcal' task change from CAS-14218
if hasattr(casatasks, 'wvrgcal'):
    # wvrgcal was migrated into the casatasks package via CAS-14218
    almatasks = casatasks
else:
    # before CAS-14218, the task wvrgcal was under the almatasks package
    import almatasks

from . import logging, utils

LOG = logging.get_logger(__name__)

# logger for keeping a trace of CASA task and CASA tool calls.
# The filename incorporates the hostname to keep MPI client files distinct
CASACALLS_LOG = logging.get_logger('CASACALLS', stream=None, format='%(message)s', addToCasaLog=False,
                                   filename='casacalls-{!s}.txt'.format(platform.node().split('.')[0]))

# functions to be executed just prior to and immediately after execution of the
# CASA task, providing a way to collect metrics on task execution.
PREHOOKS = []
POSTHOOKS = []


class FunctionArg(object):
    """
    Class to hold named function or method arguments
    """
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __str__(self):
        return '{!s}={!r}'.format(self.name, self.value)

    def __repr__(self):
        return 'FunctionArg({!r}, {!r})'.format(self.name, self.value)


class NamelessArg(object):
    """
    Class to hold unnamed arguments
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return 'NamelessArg({!r})'.format(self.value)


def alphasort(argument):
    """
    Return an argument with values sorted so that the log record is easier to
    compare to other pipeline executions.

    :param argument: the FunctionArg or NamelessArg to sort
    :return: a value-sorted argument
    """
    if isinstance(argument, NamelessArg):
        return argument

    # holds a map of argument name to separators for argument values
    attrs_and_separators = {
        'asis': ' ',
        'spw': ',',
        'field': ',',
        'intent': ','
    }

    # deepcopy as we sort in place and don't want to modify the original
    argument = copy.deepcopy(argument)
    name = argument.name
    value = argument.value

    if name == 'inpfile' and isinstance(value, list):
        # get the indices of commands that are not summaries.
        apply_cmd_idxs = [idx for idx, val in enumerate(value) if "mode='summary'" not in val]

        # group the indices into consecutive ranges, i.e., between
        # flagdata summaries. Commands within these ranges can be
        # sorted.
        for _, g in itertools.groupby(enumerate(apply_cmd_idxs), lambda i_x: i_x[0] - i_x[1]):
            idxs = list(map(operator.itemgetter(1), g))
            start_idx = idxs[0]
            end_idx = idxs[-1] + 1
            value[start_idx:end_idx] = utils.natural_sort(value[start_idx:end_idx])

    else:
        for attr_name, separator in attrs_and_separators.items():
            if name == attr_name and isinstance(value, str) and separator in value:
                value = separator.join(utils.natural_sort(value.split(separator)))

    return FunctionArg(name, value)


_uuid_regex = re.compile(r'[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}', re.I)


def UUID_to_underscore(argument):
    """
    Return an argument with UUIDs converted to underscores.

    :param argument: the FunctionArg or NamelessArg to sort
    :return: a value-sorted argument
    """
    if isinstance(argument, NamelessArg):
        return argument
    if not isinstance(argument.value, str):
        return argument

    # deepcopy as we sort in place and don't want to modify the original
    argument = copy.deepcopy(argument)

    value = _uuid_regex.sub('<UUID>', argument.value)

    return FunctionArg(argument.name, value)


def truncate_paths(arg):
    # Path arguments are kw args with specific identifiers. Exit early if this
    # is not a path argument
    if isinstance(arg, NamelessArg):
        return arg
    if arg.name not in ('vis', 'caltable', 'gaintable', 'asdm', 'outfile', 'figfile', 'listfile', 'inpfile', 'plotfile',
                        'fluxtable', 'infile', 'infiles', 'mask', 'imagename', 'fitsimage', 'outputvis'):
        return arg

    # PIPE-639: 'inpfile' is an argument for CASA's flagdata task, and it can
    # contain either a path name, a list of path names, or a list of flagging
    # commands. Attempting to get the basename of a flagging command can cause
    # it to become malformed. Treat 'inpfile' as a special case, where we only
    # return the basename if the provided string(s) resolves as a path to an
    # existing file. We cannot apply this rule to all arguments, as some
    # arguments specify output files that may not exist yet.
    func = basename_if_isfile if arg.name == 'inpfile' else os.path.basename

    # wrap value in a tuple so that strings can be interpreted by
    # the recursive map function
    basename_value = _recur_map(func, (arg.value,))[0]
    return FunctionArg(arg.name, basename_value)


def basename_if_isfile(arg: str) -> str:
    """
    Test whether input string resolves to an existing file: if so, return the
    basename of the file path, otherwise return the input string unmodified.
    """
    if os.path.isfile(arg):
        return os.path.basename(arg)
    return arg


def _recur_map(fn, data):
    return [isinstance(x, str) and fn(x) or _recur_map(fn, x) for x in data]


class JobRequest(object):
    def __init__(self, fn, *args, **kw):
        """
        Create a new JobRequest that encapsulates a function call and its
        associated arguments and keywords.
        """
        # remove any keyword arguments that have a value of None or an empty
        # string, letting CASA use the default value for that argument
        null_keywords = [k for k, v in kw.items() if v is None or (isinstance(v, str) and not v)]
        for key in null_keywords:
            kw.pop(key)

        self.fn = fn

        fn_name, is_casa_task = get_fn_name(fn)
        self.fn_name = fn_name
        if is_casa_task:
            # CASA tasks are instances rather than functions, whose execution
            # begins at __call__.
            fn = fn.__call__

        # the next piece of code does some introspection on the given function
        # so that we can find out the complete invocation, adding any implicit
        # or defaulted argument values to those arguments explicitly given. We
        # use this information if execute(verbose=True) is specified.

        # get the argument names and default argument values for the given
        # function
        argnames = list(signature(fn).parameters)
        argcount = len(argnames)
        fn_defaults = fn.__defaults__ or list()
        argdefs = dict(zip(argnames[-len(fn_defaults):], fn_defaults))

        # remove arguments that are not expected by the function, such as
        # pipeline variables that the CASA task is not expecting.
        unexpected_kw = [k for k, v in kw.items() if k not in argnames]
        if unexpected_kw:
            LOG.warning('Removing unexpected keywords from JobRequest: {!s}'.format(unexpected_kw))
            for key in unexpected_kw:
                kw.pop(key)

        self.args = args
        self.kw = kw

        self._positional = [FunctionArg(name, arg) for name, arg in zip(argnames, args)]
        self._defaulted = [FunctionArg(name, argdefs[name])
                           for name in argnames[len(args):]
                           if name not in kw and name != 'self']
        self._keyword = [FunctionArg(name, kw[name]) for name in argnames if name in kw]
        self._nameless = [NamelessArg(a) for a in args[argcount:]]

    def execute(self, verbose=False):
        """
        Execute this job, returning any result to the caller.

        :param verbose: True if the complete invocation, including all default\
            variables and arguments, should be logged instead of just those\
            explicitly given (default: False)
        :type verbose: boolean
        """
        msg = self._get_fn_msg(verbose, sort_args=False)

        for hook in PREHOOKS:
            hook(self)
        LOG.info('Executing %s' % msg)

        # log sorted arguments to facilitate easier comparisons between
        # pipeline executions
        sorted_msg = self._get_fn_msg(verbose=False, sort_args=True)
        CASACALLS_LOG.debug(sorted_msg)

        try:
            return self.fn(*self.args, **self.kw)
        finally:
            for hook in POSTHOOKS:
                hook(self)

    def _get_fn_msg(self, verbose=False, sort_args=False):
        if verbose:
            args = self._positional + self._defaulted + self._nameless + self._keyword
        else:
            args = self._positional + self._nameless + self._keyword

        processed = [truncate_paths(arg) for arg in args]
        if sort_args:
            processed = [alphasort(arg) for arg in processed]
            processed = [UUID_to_underscore(arg) for arg in processed]

        string_args = [str(arg) for arg in processed]
        return '{!s}({!s})'.format(self.fn_name, ', '.join(string_args))

    def __repr__(self):
        return 'JobRequest({!r}, {!r})'.format(self.args, self.kw)

    def __str__(self):
        return self._get_fn_msg(verbose=False, sort_args=False)

    def hash_code(self, ignore=None):
        """
        Get the numerical hash code for this JobRequest.

        This code should - but is not guaranteed - to be unique.
        """
        if ignore is None:
            ignore = []

        to_match = dict(self.kw)
        for key in ignore:
            if key in to_match:
                del to_match[key]
        return self._gen_hash(to_match)

    def _gen_hash(self, o):
        """
        Makes a hash from a dictionary, list, tuple or set to any level, that
        contains only other hashable types (including any lists, tuples, sets,
        and dictionaries).
        """
        if isinstance(o, set) or isinstance(o, tuple) or isinstance(o, list):
            return tuple([self._gen_hash(e) for e in o])

        elif not isinstance(o, dict):
            return hash(o)

        new_o = copy.deepcopy(o)
        for k, v in new_o.items():
            new_o[k] = self._gen_hash(v)

        return hash(tuple(frozenset(new_o.items())))


def get_fn_name(fn):
    """Return a tuple stating the name of the callable and whether it's a CASA task.

    :param fn: the function to inspect
    :return: (function name, bool) tuple

    Pipeline has a handful of shutil file operations wrapped up in
    JobRequests and exposed on the casa_tasks module so that they can be
    called and logged in the same manner as CASA task operations. The check
    below distinguishes CASA tasks from non-CASA code.

    Note: as of CASA ver6.5, all genuine CASA tasks are callable class instances, rather than Python functions.
    """

    for m in (casatasks, casaplotms, almatasks):
        for k in m.__all__:
            v = getattr(m, k)
            if v == fn and not isinstance(fn, types.FunctionType):
                return k, True

    return fn.__name__, False


def jobrequest_generator(func):
    """Construct a JobRequest generator for a callable.
    
    This can be used as a decorator to create a JobRequest generator for any callables so they can be
    called and logged via JobRequests.
    
    functools.wraps is used to preserve the original function's name and docstring.
    The return typing of the wrapped function is specified as JobRequest.
    
    Note that the returned JobRequest creator function can NOT be transmitted via MPI messages (e.g. in 
    the case of Tier0JobRquest) because serializing wrapped functions that are only visible in a local
    scope is not supported by Python/pickle.
    """
    if not callable(func):
        raise TypeError('fn must be a callable.')

    @functools.wraps(func)
    def job_generator(*args, **kwargs) -> JobRequest:
        """Generate a JobRequest for the given callable."""
        return JobRequest(func, *args, **kwargs)
    job_generator.__name__, _ = get_fn_name(func)

    return job_generator
