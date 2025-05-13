"""
The pipelineqa module contains base classes and plugin registry for the
pipeline's QA framework.

This module contains four key classes:

    * QAScore: the base class for QA scores
    * QAScorePool: the container for lists of QAScores
    * QAPlugin: the base class for task-specific QA handlers
    * QARegistry: the registry and manager for QA plug-ins

Tasks provide and register their own QA handlers that each extends QAPlugin.
These QA handlers analyse the results of a task, in the process adding
QA scores to the Result for display in the web log.

The pipeline QA framework is activated whenever a Results instance is accepted
into the pipeline context. The pipeline QA framework operates by calling
is_handler_for(result) on each registered QAPlugin, passing it the the accepted
Results instance for inspection. QAPlugins that claim to handle the Result are
given the Result for processing. In this step, the QA framework calls
QAPlugin.handle(context, result), the method overridden by the task-specific
QAPlugin.
"""
import abc
import collections
import enum
import operator
from typing import Optional, Set, List

from . import eventbus
from . import logging

LOG = logging.get_logger(__name__)


# QAOrigin holds information to help understand how and from where the QA scores are derived
QAOrigin = collections.namedtuple('QAOrigin', 'metric_name metric_score metric_units')

# Default origin for QAScores that do not define their own origin
NULL_ORIGIN = QAOrigin(metric_name='Unknown metric',
                       metric_score='N/A',
                       metric_units='')


class WebLogLocation(enum.Enum):
    """
    WebLogLocation is an enumeration attached to each QA score, specifying
    where in the web log this QA score should be included.
    """
    # Render output to the QA details banner at the top of the task detail page
    BANNER = enum.auto()
    # Render output to QA section of the accordion at bottom of task detail page
    ACCORDION = enum.auto()
    # Include this QA score in calculations, but do not render it
    HIDDEN = enum.auto()
    # Place this score in the banner or accordion, as appropriate. This
    # emulates < Cycle 7 behaviour.
    UNSET = enum.auto()


class TargetDataSelection:
    """
    TargetDataSelection is a struct to hold data selection metadata. Its various
    properties (vis, scan, spw, etc.) should be set to specify to which subset of
    data something applies.
    """
    def __init__(self, session: Set[str] = None, vis: Set[str] = None, scan: Set[int] = None,
                 spw: Set[int] = None, field: Set[int] = None, intent: Set[str] = None,
                 ant: Set[int] = None, pol: Set[str] = None):
        if session is None:
            session = set()
        if vis is None:
            vis = set()
        if scan is None:
            scan = set()
        if spw is None:
            spw = set()
        if field is None:
            field = set()
        if intent is None:
            intent = set()
        if ant is None:
            ant = set()
        if pol is None:
            pol = set()

        self.session = session
        self.vis = vis
        self.scan = scan
        self.spw = spw
        self.field = field
        self.intent = intent
        self.ant = ant
        self.pol = pol

    def __str__(self):
        attr_names = ['session', 'vis', 'scan', 'spw', 'field', 'intent', 'ant', 'pol']
        attr_strs = []
        for attr_name in attr_names:
            val = getattr(self, attr_name)
            if not val:
                continue
            msg = '{}={}'.format(attr_name, ','.join([str(o) for o in sorted(val)]))
            attr_strs.append(msg)
        all_attrs = ', '.join(attr_strs)
        return f'TargetDataSelection({all_attrs})'


class QAScore(object):
    def __init__(self, score, longmsg='', shortmsg='', vis=None, origin=NULL_ORIGIN,
                 weblog_location=WebLogLocation.UNSET, hierarchy='',
                 applies_to: Optional[TargetDataSelection]=None):
        """
        QAScore represent a normalised assessment of data quality.

        The QA score is normalised to the range 0.0 to 1.0. Any score outside
        this range will be truncated at presentation time to lie within this
        range.

        The long message may be rendered on the task detail page, depending on
        whether this score is considered significant and the rendering hints
        given by the weblog_location argument.

        The short message associated with a QA score is used on the task
        summary page when this task is considered representative for the stage.

        A QAScore is derived from an unnormalised metric, which should be
        provided as the 'origin' argument when available. These metrics may be
        exported to the AQUA report, depending on their significance to the
        task QA and whether AQUA metric export is enabled for the particular
        task.

        The weblog_location and hierarchy attributes are intended to be used
        together, set by appropriate task-specific aggregation logic in the
        QAPlugin code, to specify how this score should be aggregated in
        overall score calculations and if/how it should be presented in the
        weblog.

        The hierarchy attribute should be in dotted string format, e.g.,
        'amp_vs_freq.amp.slope'. QAPlugins use this metadata to organise how
        the score should be grouped for processing and aggregation.

        :param score: numeric QA score, in range 0.0 to 1.0
        :param longmsg: verbose textual description of this score
        :param shortmsg: concise textual summary of this score
        :param vis: name of measurement set assessed by this QA score (DEPRECATED)
        :param origin: metric from which this score was calculated
        :param weblog_location: destination for web log presentation of this
            score
        :param hierarchy: location in QA hierarchy, in dot-separated format
        :param applies_to: data selection covered by this QA assessment
        """
        self.score = score
        self.longmsg = longmsg
        self.shortmsg = shortmsg

        if applies_to is None:
            applies_to = TargetDataSelection()

        # if the 'vis' deprecated argument is supplied, add it to the data selection
        # TODO refactor all old uses of QAScore to supply applies_to rather than vis
        if vis is not None:
            applies_to.vis.add(vis)
        self.applies_to = applies_to

        self.origin = origin
        self.weblog_location = weblog_location
        self.hierarchy = hierarchy

    def __str__(self):
        return 'QAScore({!s}, {!r}, {!r}, {!s})'.format(self.score, self.longmsg, self.shortmsg, self.applies_to)

    def __repr__(self):
        origin = None if self.origin is NULL_ORIGIN else self.origin

        return 'QAScore({!s}, longmsg={!r}, shortmsg={!r}, origin={!r}, applies_to={!s})'.format(
            self.score, self.longmsg, self.shortmsg, origin, self.applies_to)


class QAScorePool(object):
    all_unity_longmsg = 'All QA completed successfully'
    all_unity_shortmsg = 'QA pass'

    def __init__(self):
        self.pool: List[QAScore] = []
        self._representative: Optional[QAScore] = None

    @property
    def representative(self):
        if self._representative is not None:
            return self._representative

        if not self.pool:
            return QAScore(None, 'No QA scores registered for this task', 'No QA')

        # Maybe have different algorithms here. For now just return the
        # QAScore with minimum score of all non-hidden numerical ones.
        numerical_scores = [score_obj for score_obj in scores_with_location(self.pool,
                           [WebLogLocation.ACCORDION, WebLogLocation.BANNER, WebLogLocation.UNSET])
                           if score_obj.score is not None]
        if numerical_scores:
            return min(numerical_scores, key=operator.attrgetter('score'))
        else:
            # Only None scores. Cannot select a representative one, so
            # just return the first in the list.
            return self.pool[0]

    @representative.setter
    def representative(self, value):
        self._representative = value


class QAPlugin(object, metaclass=abc.ABCMeta):
    """
    QAPlugin is the mandatory base class for all pipeline QA handlers.

    Each pipeline tasks should create its own task-specific QA handler that
    extends QAPlugin and implements the QAPlugin.handle(context, result)
    method to perform QA analysis specific to that task. New QA handlers
    should specify which type of Results classes they process by defining the
    result_cls and child_cls class properties. If the same Results class is
    returned by multiple tasks, e.g., fluxscale and setjy, then the
    generating_task class property should also be defined, which will cause
    the handler to be activated only when the Result instance is generated by
    the specified task.

    The results structure for many pipeline tasks is to return a ResultsList
    container object that contains many task-specific Results instances, one
    per EB. Two QAPlugins must be registered for this type of task: one to
    process the per-EB Results leaf objects, and another to process the
    containing ResultsList, pulling up the QA scores on each per-EB Result
    into the ResultsList's QAScorePool and setting a representative score.
    This can be achieved with two new QAPlugins, e.g.,

        # This will process the per-EB results
        MyTaskQAPlugin(QAHandler):
            result_cls = MyTaskResults
            child_cls = None

        # This will process the container
        MyTaskContainerQAPlugin(QAHandler):
            result_cls =ResultsList
            child_cls = MyTaskResults

    Within QAPlugin.handle(context, result), a QA Handler can analyse, modify,
    or make additions to the Results instances in any way it sees fit. In
    practice, the standard modification is to create and add one or more new
    QAScore instances to the QAScorePool attribute of the Result.

    Extending the QAPlugin base class automatically registers the subclass with
    with the pipeline QA framework. However, QAPlugin must be explicitly
    extended, and not implicitly inherited via another subclass of QAPlugin.
    Put another way, if class Foo extends QAPlugin, and class Bar extends Foo,
    only Foo is registered with the QA framework. To register Bar, the class
    definition must use multiple inheritance, e.g., 'class Bar(Foo, QAPlugin):'.
    """

    # the Results class this handler is expected to handle
    result_cls = None
    # if result_cls is a list, the type of classes it is expected to contain
    child_cls = None
    # the task class that generated the results, or None if it should handle
    # all results of this type regardless of which task generated it
    generating_task = None

    def is_handler_for(self, result):
        """
        Return True if this QAPlugin can process the Result.

        :param result: the task Result to inspect
        :return: True if the Result can be processed
        """
        # if the result is not a list or the expected results class,
        # return False
        if not isinstance(result, self.result_cls):
            return False

        # this is the expected class and we weren't expecting any
        # children, so we should be able to handle the result
        if self.child_cls is None and (self.generating_task is None
                                       or result.task is self.generating_task):
            return True

        try:
            if all([isinstance(r, self.child_cls) and
                    (self.generating_task is None or r.task is self.generating_task)
                    for r in result]):
                return True
            return False
        except:
            # catch case when result does not have a task attribute
            return False

    @abc.abstractmethod
    def handle(self, context, result):
        pass

    def handle_with_exception_catch(self, context, result):
        """
        Handle QA scoring with an exception catch.

        Args:
            context: Pipeline context
            result: A Pipeline task result instance
        """
        try:
            self.handle(context, result)
        except Exception as ex:
            # PIPE-2216: use a short generic message and score=-0.1 for QA execution failures and
            # present the exception error message in warning.
            result.qa.pool.append(QAScore(-0.1, 'Unable to calculate QA scores.', 'Unable to calculate QA scores'))
            LOG.warning('QA execution failed with an exception: %s', ex)

class QARegistry(object):
    """
    The registry and manager of the pipeline QA framework.

    The responsibility of the QARegistry is to pass Results to QAPlugins that
    can handle them.
    """
    def __init__(self):
        self.__plugins_loaded = False
        self.__handlers = []

    def add_handler(self, handler):
        task = handler.generating_task.__name__ if handler.generating_task else 'all'
        child_name = ''
        if hasattr(handler.child_cls, '__name__'):
            child_name = handler.child_cls.__name__
        elif isinstance(handler.child_cls, collections.abc.Iterable):
            child_name = str([x.__name__ for x in handler.child_cls])
        container = 's of %s' % child_name
        s = '%s%s results generated by %s tasks' % (handler.result_cls.__name__, container, task)
        LOG.debug('Registering %s as new pipeline QA handler for %s', handler.__class__.__name__, s)
        self.__handlers.append(handler)

    def do_qa(self, context, result):
        if not self.__plugins_loaded:
            for plugin_class in QAPlugin.__subclasses__():
                self.add_handler(plugin_class())
            self.__plugins_loaded = True

        # if this result is iterable, process the lower-level scalar results
        # first
        if isinstance(result, collections.abc.Iterable):
            for r in result:
                self.do_qa(context, r)

        # register the capturing log handler, buffering all messages so that
        # we can add them to the result - and subsequently, the weblog
        logging_handler = logging.CapturingHandler(logging.ATTENTION)
        logging.add_handler(logging_handler)

        try:
            # with the leaf results processed, the containing handler can now
            # collate the lower-level scores or process as a group
            for handler in self.__handlers:
                if handler.is_handler_for(result):
                    LOG.debug('%s handling QA analysis for %s' % (handler.__class__.__name__,
                                                                  result.__class__.__name__))
                    handler.handle_with_exception_catch(context, result)

            if hasattr(result, 'logrecords'):
                result.logrecords.extend(logging_handler.buffer)

        finally:
            # now that the messages from the QA stage have been attached to
            # the result, remove the capturing logging handler from all loggers
            logging.remove_handler(logging_handler)


def scores_with_location(pool: List[QAScore],
                         locations: Optional[List[WebLogLocation]] = None) -> List[QAScore]:
    """
    Filter QA scores by web log location.
    """
    if not locations:
        locations = list(WebLogLocation)

    return [score for score in pool if score.weblog_location in locations]


qa_registry = QARegistry()
