"""Debug wrapper decorator class."""
import functools
import inspect
import logging
import os
import pprint

import pipeline.infrastructure as infrastructure


def debugwrapper(msg: str='', pp: bool=False):
    """Decorate methods to debug

    Args:
        msg : description string to output to log
        pp : pretty-print flag of argument objects
    """
    def wrapper(func):
        @functools.wraps(func)
        def f(*args, **kwargs):
            logger = infrastructure.get_logger(func.__name__)
            retval = func(*args, **kwargs)
            if logger.level <= logging.DEBUG:
                _self = args[0]

                # check whether it wraps __setattr__()
                if func.__name__ == '__setattr__':
                    attr = args[1]
                else:
                    attr = False
                
                value = kwargs.get('value', args[-1])
                __debug(logger, _self, attr, value, msg, pp)
            return retval
        return f
    return wrapper


def __debug(logger: logging.Logger, cls: object, attr: str, obj: object, msg: str, pp: bool):
    """Output debug strings.

    Args:
        cls : caller class object
        attr : attribute when the wrapper wrapped __setattr__
        obj : object to output
        msg : action message
        pp : pretty-print flag of argument objects
    """
    outer_frames = inspect.getouterframes(inspect.currentframe())

    # step frame position outer of debugwrapper.py
    for i, frame in enumerate(inspect.getouterframes(inspect.currentframe())):
        if not frame.filename.endswith(__file__):
            break

    source = os.path.basename(outer_frames[i].filename)
    cls_name = cls.__class__.__name__
    msg_str = f'{source}[{outer_frames[i].lineno}] {msg}: {cls_name}.{outer_frames[i].function}'

    # if the wrapper wrapped __setattr__ then output the variable name
    if attr is not False:
        msg_str += f'.{attr}'

    # pretty-print
    if pp:
        obj_s = pprint.pformat(obj)
        msg_str += f':\n{type(obj)}\n{obj_s}'
    else:
        msg_str += f': {type(obj)} {obj}'

    logger.debug(msg_str)
