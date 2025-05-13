"""
This module defines common single dish Results and task container Classes.

Classes and Methods:
    SingleDishResults: Common single dish Results class.
"""

from typing import Any, Optional

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.infrastructure.utils import absolute_path

LOG = infrastructure.get_logger(__name__)


class SingleDishResults(basetask.Results):
    """
    Single Dish Results class.
    
    This class inherits a common Results class,
    pipeline.infrastructre.basetask.Results. See the documentation of the
    super class for details. The class is usually used as a superclass of
    task specific Results class.
    
    Attributes:
        task: A task class associated with this class.
        success: A boolean to indicate if the task execution was successful
            (True) or not (False).
        outcome: Outcome of the task.
        error: A set of strings to store error messages.
    """
    
    def __init__(self, task: Optional[basetask.StandardTaskTemplate]=None,
                 success: Optional[bool]=None, outcome:  Any=None):
        """
        Initialize class attributes and super class.
        
        Args:
            task: A taks class object.
            success: True if the task completes successfully. Otherwise, False.
            outcome: Outcome of the task.
        """
        super(SingleDishResults, self).__init__()
        self.task = task
        self.success = success
        self.outcome = outcome
        self.error = set()

    def merge_with_context(self, context: infrastructure.launcher.Context):
        """
        Merge these results with the given context.
        
        See the documenetation of super class for more details.
        """
        self.error.clear()

    def _outcome_name(self) -> str:
        """Return an absolute path of outcome stored as a file."""
        # usually, outcome is a name of the file
        return absolute_path(self.outcome)

    def _get_outcome(self, key: str) -> Any:
        """
        Return a value in outcome dictionary.
        
        Args:
            key: A key of outcome dictionary to get a value.
         
        Retruns:
            The value in outcome dictionary obtained by a given key.
            This method returns None, if the outcome is not a dictionary.
        """
        if isinstance(self.outcome, dict):
            return self.outcome.get(key, None)
        else:
            return None

    def __repr__(self) -> str:
        """Return a printable representation of the class."""
        # taskname = self.task if hasattr(self,'task') else 'none'
        s = '%s:\n\toutcome is %s' % (self.__class__.__name__, self._outcome_name())
        return s
