"""
The events module contains classes representing various stages in the
lifecycle of a pipeline reduction.

Events are published on a topic. The implementing class defines at a minimum
the event topic. The class may also define additional attributes identifying
and/or describing the event. These extra metadata can be interpreted by the
event listeners downstream.
"""


class Event(object):
    """
    Base class inherited by all pipeline events.
    """
    topic = ''


class ContextLifecycleEvent(Event):
    """
    Base class for events related to the lifecycle of the pipeline context.
    """
    topic = 'lifecycle.context'

    def __init__(self, context_name=None, output_dir=None):
        super().__init__()
        # Output directory should be set but can be an empty string.
        if output_dir is None:
            raise ValueError(f'output_dir unspecified when creating {self.__class__.__name__}')
        self.output_dir = output_dir
        if not context_name:
            raise ValueError(f'context_name unspecified when creating {self.__class__.__name__}')
        self.context_name = context_name


class ContextCreatedEvent(ContextLifecycleEvent):
    """
    Emitted when a pipeline Context is created.
    """
    topic = 'lifecycle.context.created'

    def __init__(self, context_name=None, output_dir=None):
        super().__init__(context_name=context_name, output_dir=output_dir)


class ContextResumedEvent(ContextLifecycleEvent):
    """
    Emitted when a pipeline Context is resumed.
    """
    topic = 'lifecycle.context.resumed'

    def __init__(self, context_name=None, output_dir=None):
        super().__init__(context_name=context_name, output_dir=output_dir)


class TaskLifecycleEvent(Event):
    """
    Base class for events related to the pipeline task lifecycle.
    """
    topic = 'lifecycle.task'

    def __init__(self, context_name, stage_number, state):
        self.context_name = context_name
        self.stage_number = stage_number
        self.state = state


class TaskStartedEvent(TaskLifecycleEvent):
    """
    Emitted when a pipeline Task is started.
    """
    topic = 'lifecycle.task.started'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'started')


class TaskCompleteEvent(TaskLifecycleEvent):
    """
    Emitted when a pipeline Task completes.
    """
    topic = 'lifecycle.task.complete'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'complete')


class TaskAbnormalExitEvent(TaskLifecycleEvent):
    """
    Emitted when a pipeline Task terminates due to an unexpected error.
    """
    topic = 'lifecycle.task.aborted'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'abnormal exit')


class WebLogLifecycleEvent(Event):
    """
    Base class for events related to weblog generation.

    This class represents the overall lifecycle encompassing generation of
    all weblog artifacts (all plots, all stages, etc.).
    """
    topic = 'lifecycle.weblog'

    def __init__(self, context_name, state):
        super().__init__()
        self.context_name = context_name
        self.state = state


class WebLogRenderingStartedEvent(WebLogLifecycleEvent):
    """
    Emitted when weblog rendering begins.
    """
    topic = 'lifecycle.weblog.rendering.started'

    def __init__(self, context_name):
        super().__init__(context_name, 'started')


class WebLogRenderingCompleteEvent(WebLogLifecycleEvent):
    """
    Emitted when weblog rendering ends.
    """
    topic = 'lifecycle.weblog.rendering.complete'

    def __init__(self, context_name):
        super().__init__(context_name, 'complete')


class WebLogStageLifecycleEvent(WebLogLifecycleEvent):
    """
    Base class for events related to weblog rendering of a specific stage.
    """
    topic = 'lifecycle.weblog.stage'

    def __init__(self, context_name, stage_number, state):
        super().__init__(context_name, state)
        self.stage_number = stage_number


class WebLogStageRenderingStartedEvent(WebLogStageLifecycleEvent):
    """
    Emitted when stage rendering begins.
    """
    topic = 'lifecycle.weblog.stage.rendering.started'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'started')


class WebLogStageRenderingCompleteEvent(WebLogStageLifecycleEvent):
    """
    Emitted when stage rendering ends.
    """
    topic = 'lifecycle.weblog.stage.rendering.complete'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'complete')


class WebLogStageRenderingAbnormalExitEvent(WebLogStageLifecycleEvent):
    """
    Emitted when stage rendering exits due to error.
    """
    topic = 'lifecycle.weblog.stage.rendering.aborted'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'abnormal exit')


class ResultLifecycleEvent(Event):
    """
    Base class for events related to the acceptance of a pipeline Result.
    """
    topic = 'lifecycle.result'

    def __init__(self, context_name, stage_number, state):
        self.context_name = context_name
        self.stage_number = stage_number
        self.state = state


class ResultAcceptingEvent(ResultLifecycleEvent):
    """
    Emitted when a Result is accepted into the Context.
    """
    topic = 'lifecycle.result.accepting'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'accepting')


class ResultAcceptedEvent(ResultLifecycleEvent):
    """
    Emitted when a Result has been successfully incorporated into the Context.
    """
    topic = 'lifecycle.result.accepted'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'accepted')


class ResultAcceptErrorEvent(TaskLifecycleEvent):
    """
    Emitted when Results acceptance fails due to error.
    """
    topic = 'lifecycle.result.error'

    def __init__(self, context_name, stage_number):
        super().__init__(context_name, stage_number, 'error')
