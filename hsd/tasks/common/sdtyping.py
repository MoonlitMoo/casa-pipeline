"""
This module defines types of objects used in single dish.
"""

from typing import Dict, NewType, TYPE_CHECKING, Union

import numpy as np

if TYPE_CHECKING:

    # N-Dimension Array of float64 
    NpArray1D = NewType('NpArray1D', np.ndarray[np.float64])
    NpArray2D = NewType('NpArray2D', np.ndarray[np.ndarray[np.float64]])
    NpArray3D = NewType('NpArray3D', np.ndarray[np.ndarray[np.ndarray[np.float64]]])

    # Direction
    # type as a direction of the origin for moving target
    Direction = NewType('Direction', Dict[str, Union[str, float]])

    # Angle
    Angle = NewType('Angle', Dict[str, Union[str, float]])
