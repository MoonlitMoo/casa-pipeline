from typing import Callable

from .h.cli import *
from .hif.cli import *
from .hifa.cli import *
from .hifv.cli import *
from .hsd.cli import *
from .hsdn.cli import *


def get_pipeline_task_with_name(task_name: str) -> Callable:
    """Return Pipeline CLI task with specified name.

    Args:
        task_name: Name of the task.

    Returns:
        Pipeline CLI task
    """
    return globals()[task_name]
