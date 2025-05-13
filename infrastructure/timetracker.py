"""
The timetracker module contains classes that track execution duration based on
the reception of lifecycle events.
"""
import collections
import datetime
import json
import os
import shelve
from contextlib import closing

from . import eventbus
from . import logging
from . import utils
from .eventbus import ContextLifecycleEvent, ContextCreatedEvent, ContextResumedEvent
from .eventbus import ResultLifecycleEvent, ResultAcceptingEvent, ResultAcceptedEvent, ResultAcceptErrorEvent
from .eventbus import TaskLifecycleEvent, TaskStartedEvent, TaskCompleteEvent, TaskAbnormalExitEvent
from .eventbus import WebLogStageLifecycleEvent, WebLogStageRenderingStartedEvent, WebLogStageRenderingCompleteEvent, \
    WebLogStageRenderingAbnormalExitEvent
from .mpihelpers import MPIEnvironment

LOG = logging.get_logger(__name__)

__all__ = ['time_tracker']

# A simple named tuple to hold start and end timestamps
ExecutionState = collections.namedtuple('ExecutionState', ['stage', 'start', 'end', 'state'])


class TaskTimeTracker(object):
    """
    TaskTimeTracker listens for pipeline lifecycle events, recording the start
    and end times on event reception so that the duration of the corresponding
    pipeline execution phase can be calculated.
    """

    def __init__(self, context_name=None, output_dir='.'):
        """
        Create a new time tracker for events affecting the named Context.

        :param context_name: Context name to match
        :param output_dir: output directory for tracker db and JSON.
        """
        self.context_name = context_name
        # shelve will add .db suffix to this filename
        self.db_path = os.path.join(output_dir, f'{context_name}.timetracker')
        self.json_path = os.path.join(output_dir, f'{context_name}.timetracker.json')
        eventbus.subscribe(self.on_task_lifecycle_event, TaskLifecycleEvent.topic)
        eventbus.subscribe(self.on_weblog_stage_lifecycle_event, WebLogStageLifecycleEvent.topic)
        eventbus.subscribe(self.on_result_lifecycle_event, ResultLifecycleEvent.topic)

    def on_lifecycle_event(self, event, db_key, start_event, stop_events, export_on=None):
        """
        Process lifecycle event, recording duration of a lifecycle phase as
        the applicable lifecycle events are received.

        :param event: lifecycle event to process
        :param db_key: category label for JSON output
        :param start_event: event that starts the clock for lifecycle
        :param stop_events: event(s) that stop the clock for lifecycle
        :param export_on: optional lifecycle event(s) to trigger export
        """
        if event.context_name != self.context_name:
            return

        now = datetime.datetime.now()
        stage_number = event.stage_number

        with closing(shelve.DbfilenameShelf(self.db_path, writeback=True)) as db:
            if db_key not in db:
                db[db_key] = {}

            if isinstance(event, start_event):
                tes = ExecutionState(stage=stage_number, start=now, end=now, state=event.state)
            elif isinstance(event, stop_events):
                old_tes = db[db_key][stage_number]
                tes = ExecutionState(stage=stage_number, start=old_tes.start, end=now, state=event.state)
            else:
                raise ValueError('Unhandled event type: {}'.format(event.__class__.__name__))

            db[db_key][stage_number] = tes

        if export_on and isinstance(event, export_on):
            self.export()

    def on_weblog_stage_lifecycle_event(self, event: WebLogStageLifecycleEvent):
        """
        Callback function for weblog stage rendering lifecycle events.
        """
        self.on_lifecycle_event(event, 'weblog', WebLogStageRenderingStartedEvent, (WebLogStageRenderingCompleteEvent, WebLogStageRenderingAbnormalExitEvent))

    def on_task_lifecycle_event(self, event: TaskLifecycleEvent):
        """
        Callback function for Task lifecycle events.
        """
        self.on_lifecycle_event(event, 'tasks', TaskStartedEvent, (TaskCompleteEvent, TaskAbnormalExitEvent))

    def on_result_lifecycle_event(self, event: ResultLifecycleEvent):
        """
        Callback function for Result lifecycle events.
        """
        self.on_lifecycle_event(event, 'results', ResultAcceptingEvent, (ResultAcceptedEvent, ResultAcceptErrorEvent),
                                export_on=(ResultAcceptedEvent, ResultAcceptErrorEvent))

    def export(self):
        """
        Exports lifecycle duration database to JSON file.
        """
        r = {}

        with closing(shelve.DbfilenameShelf(self.db_path)) as db:
            for k, stages in db.items():
                r[k] = {}
                for e in stages.values():
                    duration = e.end - e.start
                    duration_secs = duration.total_seconds()
                    duration_hms = utils.format_timedelta(duration)
                    r[k][e.stage] = {'seconds': duration_secs, 'hms': duration_hms}

            r['total'] = {}
            for stage_number, task_state in db['tasks'].items():
                try:
                    result_state = db['results'][stage_number]
                except KeyError:
                    continue
                duration = result_state.end - task_state.start
                duration_secs = duration.total_seconds()
                duration_hms = utils.format_timedelta(duration)
                r['total'][result_state.stage] = {'seconds': duration_secs, 'hms': duration_hms}

        with open(self.json_path, 'w') as json_file:
            json.dump(r, json_file, sort_keys=True, indent=4, separators=(',', ': '))


class ContextTimeTracker(object):
    """
    ContextTimeTracker listens for events related to the creation/resumption
    of a pipeline Context. As a Context is created or resumed, this class
    creates a TaskTimeTracker to record the execution duration for processes
    affecting that context.
    """
    def __init__(self):
        self.tracking = {}
        # do not track times if running on an MPI worker
        if not MPIEnvironment.is_mpi_enabled or MPIEnvironment.is_mpi_client:
            eventbus.subscribe(self.track, ContextCreatedEvent.topic)
            eventbus.subscribe(self.track, ContextResumedEvent.topic)

    def track(self, event: ContextLifecycleEvent) -> None:
        context_name = event.context_name
        LOG.info('Tracking execution duration for context: %s', context_name)
        self.tracking[context_name] = TaskTimeTracker(context_name=context_name, output_dir=event.output_dir)


time_tracker = ContextTimeTracker()
