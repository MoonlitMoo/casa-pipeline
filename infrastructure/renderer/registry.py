"""
Created on 2 Nov 2018

@author: sjw
"""
import pipeline.infrastructure.logging as logging

LOG = logging.get_logger(__name__)


class RendererRegistry(object):
    def __init__(self):
        # holds registrations of renderers that should be used in all
        # situations, unless a context-specific registration takes precedence.
        self.default_map = {}
        # holds registrations of renderers that should be used in
        # context-specific situations
        self.custom_map = {}
        # holds functions that should be used to return a context-specific
        # key, which can be used to retrieve the correct renderer for that
        # context
        self.selector_fn_map = {}

    def add_renderer(self, task_cls, renderer, key_fn=None, key=None):
        """
        Register a renderer to be used to generate HTML for results from a given
        task.

        There are two modes of registration:

        1. registration of a context-specific renderer
        2. registration of a universal renderer, which wil be used if no
           context-specific renderer is found

        Context-specific renderers are registered by supplying key and key_fn
        arguments. key_fn should be a function that accepts a context and returns
        a key from it. This key is used to look up the renderer. Specifying a
        key value of 'X' says 'this renderer should be used for this task if this
        key_fn returns 'X' for this context'.

        :param task_cls: the target pipeline Task class
        :param renderer: the renderer to use for the task
        :param key: optional key to retrieve this renderer by
        :param key_fn: optional function that accepts a pipeline context and returns the renderer key
        :return:
        """
        if key is not None and key_fn is None:
            msg = ('Renderer registration invalid for {!s}.\n'
                   'Must supply a renderer selector function when defining a renderer selector key'.format(task_cls))
            LOG.error(msg)
            raise ValueError(msg)

        # Registering without a key says that the renderer is not context
        # dependent, and the same renderer should be returned for all context
        # values.
        if key is None:
            self.default_map[task_cls] = renderer
        else:
            # Registering with a key says that the renderer is context dependent,
            # and a function should be used to extract the key value from that
            # context
            if task_cls not in self.custom_map:
                self.custom_map[task_cls] = {}
            self.custom_map[task_cls][key] = renderer
            self.selector_fn_map[task_cls] = key_fn

    def get_renderer(self, cls, context, result):
        """
        Get the registered renderer for a class.

        The pipeline context argument may be passed to a registered function that
        returns the key for the given context.

        :param cls:  the class to look up
        :param context: pipeline context
        :param result: pipeline task result
        :return: registered renderer class, or KeyError if no renderer was registered
        """
        if cls in self.custom_map:
            select_fn = self.selector_fn_map[cls]
            key = select_fn(context, result)
            if key in self.custom_map[cls]:
                return self.custom_map[cls][key]

        # either not a specific renderer or key not found, so return universal
        # renderer for that task
        return self.default_map[cls]
