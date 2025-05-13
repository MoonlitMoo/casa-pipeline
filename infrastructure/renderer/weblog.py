"""
Created on 8 Sep 2014

@author: sjw
"""
import atexit
import fnmatch
import os
import pkg_resources
import shutil
import tempfile

import mako.lookup
import mako.template

import pipeline.infrastructure.logging
import pipeline.infrastructure.renderer.templates
from .registry import RendererRegistry

LOG = pipeline.infrastructure.logging.get_logger(__name__)


# enumerations for registering web log renderers
UNGROUPED = 'ungrouped'
BY_SESSION = 'by_session'


def _get_template_lookup():
    """
    Create a Mako TemplateLookup object to which all pipeline templates will
    be registered. Compiled templates are stored in a temporary working
    directory which is deleted on process exit.
    """
    tmpdir = tempfile.mkdtemp()
    LOG.trace('Mako module directory = %s' % tmpdir)
    # Remove temporary Mako codegen directory on termination of Python process
    atexit.register(lambda: shutil.rmtree(tmpdir,
                                          ignore_errors=True))

    templates_path = pkg_resources.resource_filename(pipeline.infrastructure.renderer.templates.__name__, '')
    lookup = mako.lookup.TemplateLookup(directories=[templates_path],
                                        module_directory=tmpdir,
                                        input_encoding='utf-8',
                                        default_filters=['decode.utf8'])
    return lookup
TEMPLATE_LOOKUP = _get_template_lookup()


def register_mako_templates(directory, prefix=''):
    """
    Find Mako templates in the given directory, registering them to the module
    template lookup. Templates will be registered to a URI composed of the URI
    prefix argument (optional) plus the template filename, minus filename
    extension.

    For example, a call with prefix='hif' finding a file called
    'importdata.mako' would register the template to the Mako URI 
    'hif/importdata'.
    """
    # get relative paths to all Mako templates in the directory
    relpaths = fnmatch.filter(os.listdir(directory), '*.mako')
    # convert them to absolute paths for lookup registration
    abspaths = [os.path.join(directory, t) for t in relpaths]

    if directory not in TEMPLATE_LOOKUP.directories:
        TEMPLATE_LOOKUP.directories.append(directory)

    # # TODO replace with explicit registration for control over URIs
    # for template_path in abspaths:
    #     # postponed until task import is removed from htmlrenderer
    #     root, _ = os.path.splitext(os.path.basename(template_path))
    #     uri = os.path.join(prefix, root)
    #
    #     t = mako.template.Template(filename=template_path,
    #                                format_exceptions=True,
    #                                module_directory=TEMPLATE_LOOKUP.module_directory,
    #                                lookup=TEMPLATE_LOOKUP,
    #                                uri=uri)
    #
    #     TEMPLATE_LOOKUP.put_template(uri, t)
    #     LOG.trace('%s registered to URI %s', template_path, uri)


class WebLogRendererRegistry(RendererRegistry):
    def __init__(self):
        super(WebLogRendererRegistry, self).__init__()
        self._render_by_session = set()
        self._render_ungrouped = set()

    def add_renderer(self, task_cls, renderer, group_by=None, key_fn=None, key=None):
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
        :param group_by: grouping directive - either "session" or "ungrouped"
        :param key: optional key to retrieve this renderer by
        :param key_fn: optional function that accepts a pipeline context and returns the renderer key
        :return:
        """
        super(WebLogRendererRegistry, self).add_renderer(task_cls, renderer, key_fn, key)

        if group_by == 'session':
            self._render_by_session.add(task_cls.__name__)
        elif group_by == 'ungrouped':
            self._render_ungrouped.add(task_cls.__name__)
        else:
            LOG.warning('{} did not register a renderer group type. Assuming it is grouped by '
                        'session'.format( task_cls.__name__))
            self._render_by_session.add(task_cls.__name__)

    def render_by_session(self, task_name):
        return task_name in self._render_by_session

    def render_ungrouped(self, task_name):
        return task_name in self._render_ungrouped


registry = WebLogRendererRegistry()


# this function exists at the module level to retain compatibility with
# pre-RenderRegistry code.
def add_renderer(*args, **kwargs):
    registry.add_renderer(*args, **kwargs)
