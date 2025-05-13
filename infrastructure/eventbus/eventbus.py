"""
The eventbus module defines the interface for publishing events and
subscribing to events.
"""
from typing import Callable, TypeVar

from pubsub import pub

from .events import Event

E = TypeVar('E', bound=Event)


def subscribe(fn: Callable[[E], None], topic: str):
    """
    Request callback when a lifecycle event on a topic is published.

    The subscribing function should accept a single Event argument.

    :param fn: callback function
    :param topic: event topic to match
    """
    pub.subscribe(fn, topic)


def send_message(event: E):
    """
    Publish an event.
    """
    pub.sendMessage(event.topic, event=event)
